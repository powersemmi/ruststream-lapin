// The benchmark is a binary of its own, not library surface: a measured loop panics on a broker
// fault rather than threading a `Result` through a scenario nobody recovers from.
#![allow(
    missing_docs,
    unreachable_pub,
    unused_qualifications,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
//! What this crate, and then the runtime above it, cost over the `lapin` client they wrap.
//!
//! Every scenario runs three times over, and the three runs differ in one thing each: what
//! carries the messages.
//!
//! - `raw` drives `lapin` directly: its consumer, its `basic.publish`.
//! - `adapter` drives this crate and nothing else - [`LapinBroker`], the queue descriptor as a
//!   subscription source, the [`LapinSubscriber`] stream it yields, the delivery's own `ack`, and
//!   [`LapinPublish`] paired into a publisher. A loop here pulls that stream, decodes, reads a
//!   field and settles. No handler, no app, no dispatch.
//! - `framework` is the service a user writes: a `#[subscriber]` handler, the app, the runtime,
//!   over the same publisher the adapter used.
//!
//! `adapter` against `raw` is what this crate's own consumer and publisher cost over the client
//! they wrap. `framework` against `adapter` is what the runtime costs on top of them, over this
//! broker in particular: the adapters across the family are all thin, so a runtime share that
//! differs from one broker to the next is a fact about how the two meet here.
//!
//! The procedure the numbers follow is the framework's own, published at
//! <https://powersemmi.github.io/ruststream/latest/benchmarks/>.
//!
//! # What a run is
//!
//! The consumer is attached first, publishers on connections of their own then feed it, and the
//! window runs from the first delivery to the end of the work on the last. Connecting,
//! declaring the queue and registering the consumer are startup cost and sit outside it. Every run
//! declares a fresh queue and deletes it afterwards, so a run never sees what the one before it
//! left behind.
//!
//! The message count is not a constant: a probe run measures the raw half's rate and the count is
//! set from it, so a measured run lasts at least [`SECONDS`] on whatever machine it is taken on.
//!
//! The three are interleaved round after round (raw, adapter, framework, raw, adapter, framework)
//! and each reports its best, median and worst round. The best is the headline: noise only ever
//! slows a run down, so the fastest round is the closest to the undisturbed cost. The distance
//! between the best and the worst is the noise a difference has to clear. Running one to the end
//! and then the next would charge every drift of the machine to whichever ran last.
//!
//! # What the numbers do not say
//!
//! A row is marked broker-bound when the raw client spent the run waiting on the transport rather
//! than working. That is decided from a measurement, never from a guess: [`round_trip`] times one
//! request the server answers, outside every pair, and the row is marked when the round trips a
//! delivery costs ([`round_trips_per_delivery`]) come to at least half the time a message took.
//! The figure it measured is published with the results, so the arithmetic can be checked.

use std::convert::Infallible;
use std::env;
use std::fmt::Write as _;
use std::hint::black_box;
use std::iter::repeat_n;
use std::num::{NonZeroU16, NonZeroUsize};
use std::pin::pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures::StreamExt;
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, BasicQosOptions,
    QueueDeclareOptions, QueueDeleteOptions,
};
use lapin::types::ShortString;
use lapin::{BasicProperties, Channel, Connection, ConnectionProperties};
use ruststream::runtime::RunningApp;
use ruststream::{ConnectedBroker, OutgoingMessage, Subscriber, SubscriptionSource};
use ruststream_lapin::prelude::*;
use ruststream_lapin::{ConnectedLapinBroker, LapinSubscriber};
use serde::Deserialize;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::Notify;
use tokio::time::{sleep, timeout};

// A benchmark measures what ships, and the framework's harness feature changes what this crate's
// own types do on the delivery path. The benchmark lives in a package of its own for the same
// reason: `ruststream-lapin`'s dev-dependencies pull `ruststream/conformance`, which enables
// `ruststream/testing`, and a benchmark inside that package would link it.
#[cfg(feature = "testing")]
compile_error!(
    "benchmarks must be built without the `testing` feature; run them through `just bench`"
);

/// Deliveries the probe run takes to measure the raw half's rate.
const PROBE_MESSAGES: usize = 50_000;
/// How long a measured run lasts, at least.
const SECONDS: f64 = 5.0;
/// How much the calibrated count is raised above the probe's estimate.
///
/// The probe is short and cold, so it reads the machine low; without the margin the fastest
/// scenario lands just under the floor.
const MARGIN: f64 = 1.25;
/// The ceiling on a calibrated count, so a machine an order faster does not turn a run into an
/// afternoon.
const MAX_MESSAGES: usize = 5_000_000;
/// Rounds run. Each loop reports its best, median and worst round.
const PAIRS: usize = 3;
/// Worker threads both halves are driven on.
const WORKERS: usize = 4;

/// Unacknowledged deliveries the server pushes before it waits (`basic.qos`).
///
/// The one setting that decides what an ack-each consumer can reach on AMQP: a window of one turns
/// every message into a round trip, and a window this wide keeps the socket full while still
/// bounding what an unsettled consumer holds. Both halves are given it, the adapter through
/// [`LapinBroker::prefetch`] and the hand-written loop through `basic.qos` on its own channel.
const PREFETCH: NonZeroU16 = nonzero!(512);

/// Connections the run's bodies are published over.
///
/// One is not enough: a single publish loop tops out around the rate one consumer can settle, and
/// a run its own feed paced would report the feed's speed for both halves alike. Four connections
/// keep the queue ahead of the consumer, so what the window measures is what drains it.
const PUBLISHERS: usize = 4;
/// How far the publishers may run ahead of the consumer, in messages.
///
/// A queue takes whatever is published to it, so without a ceiling a fast feed would put the whole
/// run in the broker before the consumer had drained a tenth of it, and the number would be about
/// a queue draining rather than about a consumer consuming. 32768 bodies is 16 MiB outstanding,
/// far more than either half of a pair is ever behind.
const IN_FLIGHT: usize = 32_768;
/// How often a feed checks that ceiling.
const CHECK_EVERY: usize = 512;
/// Requests the round-trip probe times.
const ROUND_TRIPS: usize = 20_000;
/// Requests the probe throws away first, so the connection's own warm-up is not in the figure.
const ROUND_TRIP_WARMUP: usize = 2_000;
/// How long a run may go without a delivery before it is called stuck.
const STALL: Duration = Duration::from_secs(30);

/// The body size both halves publish and decode, to the byte: the scenario is published under
/// this number, so the bytes on the wire have to be it.
const BODY_BYTES: usize = 512;
/// How wide one padding value is before the next field starts.
const PAD_WIDTH: usize = 16;
/// The values every body carries. Fixed, so every delivery of a run costs the same.
const ID: u64 = 1_000_000;
const QUANTITY: u32 = 37;

/// What both halves decode a delivery into.
///
/// Two integer fields the loop reads, and a padding the type ignores: a decode that allocates
/// nothing, so the number is about this crate rather than about `serde_json`'s string handling.
#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
    quantity: u32,
}

/// A JSON body carrying the two fields, padded with fields [`Order`] ignores until it is exactly
/// `size` bytes.
///
/// The padding is a run of equally wide fields and one last field cut to whatever is left, so a
/// scenario published as a 512 byte body is one. Building it is startup work, and the assertion
/// below holds the promise the published name makes.
fn json_body(size: usize) -> Vec<u8> {
    let mut body = format!("{{\"id\":{ID},\"quantity\":{QUANTITY}");
    let mut field = 0u32;
    loop {
        let key = format!(",\"f{field}\":\"\"");
        // One byte stays reserved for the closing brace.
        let Some(room) = size.checked_sub(body.len() + key.len() + 1) else {
            break;
        };
        // A full-width field only when what it leaves behind can still hold the next one, whose
        // key is at most one digit longer. Otherwise this is the last field and it takes the
        // rest, because a remainder too small to start a field would come out as a short body.
        let width = if room > PAD_WIDTH + key.len() {
            PAD_WIDTH
        } else {
            room
        };
        body.push_str(&key[..key.len() - 1]);
        body.extend(repeat_n('x', width));
        body.push('"');
        field += 1;
    }
    body.push('}');
    assert_eq!(
        body.len(),
        size,
        "a body has to be the size the scenario publishes"
    );
    body.into_bytes()
}

/// The queue one run owns: nothing is shared with the run before it.
fn fresh_queue() -> String {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|since| since.as_nanos())
        .unwrap_or_default();
    format!("ruststream.bench.{stamp}")
}

/// Counts deliveries and marks the ends of the measured window.
///
/// Both halves call the same methods, so both pay for the signal. A delivery pays one relaxed
/// increment and two comparisons; the waiter is a single future for the whole run, woken once.
#[derive(Clone, Debug)]
struct Run(Arc<RunInner>);

#[derive(Debug)]
struct RunInner {
    total: usize,
    seen: AtomicUsize,
    first: OnceLock<Instant>,
    last: OnceLock<Instant>,
    drained: Notify,
}

impl Run {
    fn new(total: usize) -> Self {
        Self(Arc::new(RunInner {
            total,
            seen: AtomicUsize::new(0),
            first: OnceLock::new(),
            last: OnceLock::new(),
            drained: Notify::new(),
        }))
    }

    /// Records one handled delivery, and answers whether the run is over.
    fn arrived(&self) -> bool {
        let seen = self.0.seen.fetch_add(1, Ordering::Relaxed) + 1;
        if seen == 1 {
            let _ = self.0.first.set(Instant::now());
        }
        if seen == self.0.total {
            let _ = self.0.last.set(Instant::now());
            self.0.drained.notify_one();
        }
        seen >= self.0.total
    }

    fn handled(&self) -> usize {
        self.0.seen.load(Ordering::Acquire).min(self.0.total)
    }

    /// Resolves once every expected delivery has been handled.
    async fn drained(&self) {
        while self.0.seen.load(Ordering::Acquire) < self.0.total {
            self.0.drained.notified().await;
        }
    }

    /// The measured window: the first delivery to the end of the loop body that took the last.
    fn window(&self) -> Duration {
        let first = *self.0.first.get().expect("the run took a delivery");
        let last = *self.0.last.get().expect("the run took its last delivery");
        last - first
    }
}

/// Waits for the run to finish, and fails with what it was waiting for if it stops moving.
async fn drain(run: &Run, half: &str) {
    let mut seen = 0;
    loop {
        if timeout(STALL, run.drained()).await.is_ok() {
            return;
        }
        let handled = run.handled();
        assert!(
            handled > seen,
            "{half}: {handled} of {} deliveries handled and nothing moved for {STALL:?}",
            run.0.total
        );
        seen = handled;
    }
}

/// Deliveries a second, from the window one half of a pair measured.
fn rate(window: Duration, messages: usize) -> f64 {
    messages as f64 / window.as_secs_f64()
}

/// Round trips one delivery costs the consumer on this transport.
///
/// AMQP pushes a body and takes an acknowledgement, and the server answers neither frame, so a
/// delivery waits for nothing of its own. What the consumer does wait for is the credit its
/// acknowledgement returns: the server sends the next body one round trip later, and a prefetch
/// window of N spreads that single wait over N deliveries.
fn round_trips_per_delivery() -> f64 {
    1.0 / f64::from(PREFETCH.get())
}

/// What one request the client waits for costs on this transport, measured outside every pair.
///
/// `basic.qos` is the cheapest synchronous method AMQP 0.9.1 has: one frame out, one `qos-ok`
/// back, and nothing for the server to do but write down a number. The figure decides whether a
/// row is the transport's speed rather than this crate's, so it is published with the results.
async fn round_trip(url: &str) -> Duration {
    let (connection, channel) = connect(url).await;
    for _ in 0..ROUND_TRIP_WARMUP {
        channel
            .basic_qos(PREFETCH.get(), BasicQosOptions::default())
            .await
            .expect("the server answers basic.qos");
    }
    let start = Instant::now();
    for _ in 0..ROUND_TRIPS {
        channel
            .basic_qos(PREFETCH.get(), BasicQosOptions::default())
            .await
            .expect("the server answers basic.qos");
    }
    let elapsed = start.elapsed();
    close(connection).await;
    elapsed / ROUND_TRIPS as u32
}

// ---------------------------------------------------------------------------------------------
// The feed
// ---------------------------------------------------------------------------------------------

/// A connection and the channel this crate opens together with it.
///
/// `LapinBroker::connect` opens a connection and one channel on it before anything is subscribed,
/// so the hand-written half opens the same pair: a channel is a server-side resource, and a
/// benchmark that gave one half fewer of them would be comparing two topologies.
async fn connect(url: &str) -> (Connection, Channel) {
    let connection = Connection::connect(url, ConnectionProperties::default())
        .await
        .expect("the RabbitMQ node accepts a connection");
    let channel = connection
        .create_channel()
        .await
        .expect("the connection opens a channel");
    (connection, channel)
}

async fn close(connection: Connection) {
    connection
        .close(0, ShortString::default())
        .await
        .expect("the connection closes");
}

/// How many bodies each of the [`PUBLISHERS`] feeds sends, adding up to `messages` exactly.
fn shares(messages: usize) -> impl Iterator<Item = usize> {
    (0..PUBLISHERS)
        .map(move |index| messages / PUBLISHERS + usize::from(index < messages % PUBLISHERS))
}

/// Holds a feed back while the consumer is more than [`IN_FLIGHT`] messages behind it.
async fn wait_for_room(queued: &AtomicUsize, run: &Run) {
    while queued.load(Ordering::Relaxed).saturating_sub(run.handled()) > IN_FLIGHT {
        sleep(Duration::from_micros(200)).await;
    }
}

/// The hand-written feed: `basic.publish` to the default exchange under the queue's own name.
async fn raw_publish_all(url: &str, queue: &str, messages: usize, run: &Run) {
    let body = Arc::new(json_body(BODY_BYTES));
    let queued = Arc::new(AtomicUsize::new(0));
    let mut feeds = Vec::with_capacity(PUBLISHERS);
    for share in shares(messages) {
        let (connection, channel) = connect(url).await;
        let routing_key = ShortString::from(queue);
        let body = Arc::clone(&body);
        let queued = Arc::clone(&queued);
        let run = run.clone();
        feeds.push(tokio::spawn(async move {
            let exchange = ShortString::default();
            for sent in 0..share {
                if sent % CHECK_EVERY == 0 {
                    wait_for_room(&queued, &run).await;
                }
                let _confirm = channel
                    .basic_publish(
                        exchange.clone(),
                        routing_key.clone(),
                        BasicPublishOptions::default(),
                        &body,
                        BasicProperties::default(),
                    )
                    .await
                    .expect("the server accepts the publish");
                queued.fetch_add(1, Ordering::Relaxed);
            }
            close(connection).await;
        }));
    }
    for feed in feeds {
        feed.await.expect("the publishing task ends");
    }
}

/// The same feed through this crate: [`LapinPublish`] paired with a connected broker, one message
/// at a time through the publisher it produced.
async fn adapter_publish_all(url: &str, queue: &str, messages: usize, run: &Run) {
    let body = Arc::new(json_body(BODY_BYTES));
    let queued = Arc::new(AtomicUsize::new(0));
    let mut feeds = Vec::with_capacity(PUBLISHERS);
    for share in shares(messages) {
        let connected = LapinBroker::new(url)
            .connect()
            .await
            .expect("the RabbitMQ node accepts a connection");
        let publisher = LapinPublish::default()
            .pair(&connected)
            .await
            .expect("the policy pairs with the connection");
        let queue = queue.to_owned();
        let body = Arc::clone(&body);
        let queued = Arc::clone(&queued);
        let run = run.clone();
        feeds.push(tokio::spawn(async move {
            for sent in 0..share {
                if sent % CHECK_EVERY == 0 {
                    wait_for_room(&queued, &run).await;
                }
                publisher
                    .publish(OutgoingMessage::new(&queue, &body), None)
                    .await
                    .expect("the server accepts the publish");
                queued.fetch_add(1, Ordering::Relaxed);
            }
            connected.shutdown().await.expect("the connection closes");
        }));
    }
    for feed in feeds {
        feed.await.expect("the publishing task ends");
    }
}

/// Takes the run's queue away again, on a connection of its own: the run that declared it has
/// stopped by now, and this is teardown rather than anything the window covers.
async fn delete_queue(url: &str, queue: &str) {
    let (connection, channel) = connect(url).await;
    channel
        .queue_delete(ShortString::from(queue), QueueDeleteOptions::default())
        .await
        .expect("the queue is deleted");
    close(connection).await;
}

// ---------------------------------------------------------------------------------------------
// The hand-written half
// ---------------------------------------------------------------------------------------------

/// The declaration this crate sends for a descriptor of `kind` with nothing set on it, spelled
/// out here so the two halves ask the server for the same queue.
///
/// Both descriptors start durable, shared and permanent, and both name their implementation in
/// `x-queue-type` - the classic one explicitly, which the server accepts as the value it would
/// have defaulted to.
async fn declare_queue(channel: &Channel, queue: &str, kind: &str) {
    let mut arguments = FieldTable::default();
    arguments.insert(
        ShortString::from("x-queue-type"),
        AMQPValue::LongString(kind.into()),
    );
    channel
        .queue_declare(
            ShortString::from(queue),
            QueueDeclareOptions {
                durable: true,
                ..QueueDeclareOptions::default()
            },
            arguments,
        )
        .await
        .expect("the queue is declared");
}

/// The hand-written consumer: `lapin` directly, doing what this crate does around a delivery and
/// nothing more.
async fn raw(url: &str, queue: &str, kind: &str, messages: usize) -> Duration {
    let (connection, _shared) = connect(url).await;
    // A subscription gets a channel of its own, the way `ConnectedLapinBroker::subscribe` opens
    // one: the shared channel above is the publish side and is never consumed on.
    let channel = connection
        .create_channel()
        .await
        .expect("the connection opens the subscription channel");
    declare_queue(&channel, queue, kind).await;
    channel
        .basic_qos(PREFETCH.get(), BasicQosOptions::default())
        .await
        .expect("the server accepts the prefetch window");
    let mut deliveries = channel
        .basic_consume(
            ShortString::from(queue),
            ShortString::default(),
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("the server accepts the consumer");

    let run = Run::new(messages);
    let consuming = tokio::spawn({
        let run = run.clone();
        async move {
            while let Some(delivery) = deliveries.next().await {
                let delivery = delivery.expect("the consumer delivers");
                let order: Order =
                    serde_json::from_slice(&delivery.data).expect("the body decodes");
                black_box((order.id, order.quantity));
                // The window closes before the acknowledgement on both halves: this crate settles
                // a delivery once the work on it is done, and so does this loop.
                let done = run.arrived();
                delivery
                    .acker
                    .ack(BasicAckOptions::default())
                    .await
                    .expect("the ack reaches the server");
                if done {
                    break;
                }
            }
        }
    });

    raw_publish_all(url, queue, messages, &run).await;
    drain(&run, "raw").await;
    consuming.await.expect("the consuming task ends");
    let window = run.window();
    close(connection).await;
    delete_queue(url, queue).await;
    window
}

// ---------------------------------------------------------------------------------------------
// The measured half
// ---------------------------------------------------------------------------------------------

/// The broker both measured halves subscribe through: the prefetch window the raw loop sets on
/// its own channel, and the declaration that creates this run's queue.
fn broker(url: &str) -> LapinBroker {
    LapinBroker::new(url)
        .prefetch(PREFETCH)
        .declare_topology(true)
}

/// Pulls the subscription's stream to the end of the run, decoding and settling every delivery.
///
/// This is the whole measured half: no handler, no dispatch, no service - a loop over the stream
/// this crate's subscriber yields, doing exactly what the hand-written loop does.
async fn consume(mut subscriber: LapinSubscriber, run: Run) {
    let mut deliveries = pin!(subscriber.stream());
    while let Some(message) = deliveries.next().await {
        let message = message.expect("the subscription yields a delivery");
        let order: Order = serde_json::from_slice(message.payload()).expect("the body decodes");
        black_box((order.id, order.quantity));
        let done = run.arrived();
        message.ack().await.expect("the ack reaches the server");
        if done {
            break;
        }
    }
}

/// Opens this run's subscription through the descriptor the scenario names.
///
/// The two descriptors are different types, so the branch is here rather than behind a value: a
/// subscription source resolves against the connected broker, and what it yields is the same
/// subscriber either way.
async fn subscribe(
    connected: &ConnectedLapinBroker,
    scenario: Scenario,
    queue: &str,
) -> LapinSubscriber {
    match scenario {
        Scenario::Classic => RabbitQueue::new(queue).subscribe(connected).await,
        Scenario::Quorum => RabbitQuorumQueue::new(queue).subscribe(connected).await,
    }
    .expect("the subscription opens")
}

/// The queue the service being built subscribes to.
///
/// `#[subscriber(..)]` takes an expression and evaluates it where the handler is mounted, which is
/// inside the builder of the run that is starting. A run installs its own queue here first, so the
/// subscription the service opens is the one this run publishes to.
static QUEUE: Mutex<Option<String>> = Mutex::new(None);

fn install(queue: &str) {
    *QUEUE
        .lock()
        .expect("the name cell is never held across a panic") = Some(queue.to_owned());
}

fn installed() -> String {
    QUEUE
        .lock()
        .expect("the name cell is never held across a panic")
        .clone()
        .expect("a run installs its queue before it builds the service")
}

#[subscriber(RabbitQueue::new(installed()))]
async fn classic_handler(order: &Order, ctx: &mut Context<'_, (), Run>) -> HandlerOutcome {
    black_box((order.id, order.quantity));
    ctx.state().arrived();
    HandlerOutcome::ack()
}

#[subscriber(RabbitQuorumQueue::new(installed()))]
async fn quorum_handler(order: &Order, ctx: &mut Context<'_, (), Run>) -> HandlerOutcome {
    black_box((order.id, order.quantity));
    ctx.state().arrived();
    HandlerOutcome::ack()
}

async fn start(url: &str, scenario: Scenario, run: Run) -> RunningApp {
    let app = RustStream::new(AppInfo::new("lapin-bench", "0.0.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(run));
    match scenario {
        Scenario::Classic => app.with_broker(broker(url), |b| {
            b.include(classic_handler);
        }),
        Scenario::Quorum => app.with_broker(broker(url), |b| {
            b.include(quorum_handler);
        }),
    }
    .start()
    .await
    .expect("the service starts")
}

/// The whole service a user writes: the handler, the app, the runtime, over the same publisher
/// the adapter run used.
async fn framework(url: &str, queue: &str, scenario: Scenario, messages: usize) -> Duration {
    let run = Run::new(messages);
    install(queue);
    let app = start(url, scenario, run.clone()).await;

    adapter_publish_all(url, queue, messages, &run).await;
    drain(&run, "framework").await;
    app.shutdown().await.expect("the service stops");
    let window = run.window();
    delete_queue(url, queue).await;
    window
}

/// The consumer and publisher this crate ships, driven by loops of the benchmark's own.
async fn adapter(url: &str, queue: &str, scenario: Scenario, messages: usize) -> Duration {
    let connected = broker(url)
        .connect()
        .await
        .expect("the RabbitMQ node accepts a connection");
    let subscriber = subscribe(&connected, scenario, queue).await;

    let run = Run::new(messages);
    let consuming = tokio::spawn(consume(subscriber, run.clone()));

    adapter_publish_all(url, queue, messages, &run).await;
    drain(&run, "adapter").await;
    consuming.await.expect("the consuming task ends");
    let window = run.window();
    connected.shutdown().await.expect("the connection closes");
    delete_queue(url, queue).await;
    window
}

// ---------------------------------------------------------------------------------------------
// The run
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
enum Scenario {
    Classic,
    Quorum,
}

impl Scenario {
    const fn name(self) -> &'static str {
        match self {
            Self::Classic => "Classic queue, 512 B JSON, ack each",
            Self::Quorum => "Quorum queue, 512 B JSON, ack each",
        }
    }

    /// The `x-queue-type` the descriptor of this scenario declares.
    const fn kind(self) -> &'static str {
        match self {
            Self::Classic => "classic",
            Self::Quorum => "quorum",
        }
    }

    async fn raw(self, url: &str, queue: &str, messages: usize) -> Duration {
        raw(url, queue, self.kind(), messages).await
    }

    async fn adapter(self, url: &str, queue: &str, messages: usize) -> Duration {
        adapter(url, queue, self, messages).await
    }

    async fn framework(self, url: &str, queue: &str, messages: usize) -> Duration {
        framework(url, queue, self, messages).await
    }
}

/// Best, median and worst of the rounds.
///
/// Noise on the machine only ever slows a run down, so the fastest round is the closest to the
/// undisturbed cost, the median is the typical one, and the slowest says how far from quiet the
/// machine was.
#[derive(Clone, Copy, Debug)]
struct Stats {
    best: f64,
    median: f64,
    worst: f64,
}

impl Stats {
    fn of(rates: &[f64]) -> Self {
        assert!(!rates.is_empty(), "no round was run");
        let mut sorted = rates.to_vec();
        sorted.sort_by(f64::total_cmp);
        let middle = sorted.len() / 2;
        let median = if sorted.len() % 2 == 1 {
            sorted[middle]
        } else {
            f64::midpoint(sorted[middle - 1], sorted[middle])
        };
        Self {
            best: sorted[sorted.len() - 1],
            median,
            worst: sorted[0],
        }
    }

    fn spread(self) -> f64 {
        self.best - self.worst
    }
}

#[derive(Debug)]
struct Measured {
    scenario: Scenario,
    messages: usize,
    pairs: usize,
    raw: Stats,
    adapter: Stats,
    framework: Stats,
    overhead_percent: f64,
    adapter_overhead_percent: f64,
    verdict: &'static str,
    adapter_verdict: &'static str,
    broker_bound: bool,
}

/// How much slower `measured` is than `raw`, and whether the difference is worth publishing.
///
/// A difference smaller than the run-to-run spread of either side is a verdict rather than a
/// percentage: the honesty rule of the procedure, applied to each of the two differences the row
/// carries.
fn difference(raw: Stats, measured: Stats) -> (f64, &'static str) {
    let verdict = if (raw.best - measured.best).abs() < raw.spread().max(measured.spread()) {
        "indistinguishable"
    } else {
        "measured"
    };
    ((raw.best - measured.best) / raw.best * 100.0, verdict)
}

async fn measure(
    scenario: Scenario,
    url: &str,
    pairs: usize,
    seconds: f64,
    round_trip: Duration,
) -> Measured {
    // The probe is the warm-up as well: its result is thrown away, and the rate it measured sets
    // a count that makes every run below last at least `seconds`.
    let probe = rate(
        scenario.raw(url, &fresh_queue(), PROBE_MESSAGES).await,
        PROBE_MESSAGES,
    );
    let messages = ((probe * seconds * MARGIN) as usize).clamp(PROBE_MESSAGES, MAX_MESSAGES);
    println!(
        "{}: {messages} messages per run ({probe:.0} msg/s probed)",
        scenario.name(),
    );

    let mut raws = Vec::with_capacity(pairs);
    let mut adapters = Vec::with_capacity(pairs);
    let mut frameworks = Vec::with_capacity(pairs);
    for round in 1..=pairs {
        let raw = rate(scenario.raw(url, &fresh_queue(), messages).await, messages);
        let adapter = rate(
            scenario.adapter(url, &fresh_queue(), messages).await,
            messages,
        );
        let framework = rate(
            scenario.framework(url, &fresh_queue(), messages).await,
            messages,
        );
        println!(
            "  round {round:>2}: raw {raw:>10.0}, adapter {adapter:>10.0},              framework {framework:>10.0} msg/s"
        );
        raws.push(raw);
        adapters.push(adapter);
        frameworks.push(framework);
    }

    let raw = Stats::of(&raws);
    let adapter = Stats::of(&adapters);
    let framework = Stats::of(&frameworks);
    let (adapter_overhead_percent, adapter_verdict) = difference(raw, adapter);
    let (overhead_percent, verdict) = difference(raw, framework);
    // What the raw client could not have spent on anything but the transport: the round trips a
    // delivery costs it, at the latency the probe measured. Half the time a message took is the
    // line above which the row says more about the server than about this crate.
    let waiting = round_trips_per_delivery() * round_trip.as_secs_f64();
    Measured {
        scenario,
        messages,
        pairs,
        raw,
        adapter,
        framework,
        overhead_percent,
        adapter_overhead_percent,
        verdict,
        adapter_verdict,
        broker_bound: waiting >= 0.5 / raw.best,
    }
}

fn document(measured: &[Measured], round_trip: Duration) -> String {
    // The probe's figure travels with the scenarios so the published environment can carry it:
    // the rule that marks a row broker-bound is arithmetic over it, and a reader checks the
    // arithmetic only if the input is published too.
    let mut out = format!(
        "{{\n  \"round_trip_us\": {:.1},\n  \"scenarios\": [\n",
        round_trip.as_secs_f64() * 1e6
    );
    for (index, row) in measured.iter().enumerate() {
        let comma = if index + 1 == measured.len() { "" } else { "," };
        write!(
            out,
            concat!(
                "    {{\n",
                "      \"name\": \"{name}\",\n",
                "      \"unit\": \"msg/s\",\n",
                "      \"messages\": {messages},\n",
                "      \"pairs\": {pairs},\n",
                "      \"raw\": {{ \"best\": {raw_best:.0}, \"median\": {raw_median:.0}, \"worst\": {raw_worst:.0} }},\n",
                "      \"adapter\": {{ \"best\": {ad_best:.0}, \"median\": {ad_median:.0}, \"worst\": {ad_worst:.0} }},\n",
                "      \"framework\": {{ \"best\": {fw_best:.0}, \"median\": {fw_median:.0}, \"worst\": {fw_worst:.0} }},\n",
                "      \"adapter_overhead_percent\": {adapter_overhead:.1},\n",
                "      \"overhead_percent\": {overhead:.1},\n",
                "      \"adapter_verdict\": \"{adapter_verdict}\",\n",
                "      \"verdict\": \"{verdict}\",\n",
                "      \"broker_bound\": {broker_bound}\n",
                "    }}{comma}\n",
            ),
            name = row.scenario.name(),
            messages = row.messages,
            pairs = row.pairs,
            raw_best = row.raw.best,
            raw_median = row.raw.median,
            raw_worst = row.raw.worst,
            ad_best = row.adapter.best,
            ad_median = row.adapter.median,
            ad_worst = row.adapter.worst,
            fw_best = row.framework.best,
            fw_median = row.framework.median,
            fw_worst = row.framework.worst,
            adapter_overhead = row.adapter_overhead_percent,
            overhead = row.overhead_percent,
            adapter_verdict = row.adapter_verdict,
            verdict = row.verdict,
            broker_bound = row.broker_bound,
            comma = comma,
        )
        .expect("writing to a String");
    }
    out.push_str("  ]\n}\n");
    out
}

fn runtime() -> Runtime {
    Builder::new_multi_thread()
        .worker_threads(WORKERS)
        .enable_all()
        .build()
        .expect("the tokio runtime builds")
}

/// A positive count from the environment, or the default.
///
/// The parse target rejects zero, so a pairs count of zero is refused here rather than after the
/// probe run, where it would panic in the statistics with no round to report.
fn number(name: &str, fallback: usize) -> usize {
    env::var(name).ok().map_or(fallback, |value| {
        value
            .parse::<NonZeroUsize>()
            .unwrap_or_else(|_| panic!("{name} must be a positive number"))
            .get()
    })
}

fn main() {
    let url = env::var("AMQP_TEST_URL")
        .expect("AMQP_TEST_URL names the node to measure against; `just bench` sets it");
    let pairs = number("RUSTSTREAM_BENCH_PAIRS", PAIRS);
    let seconds = number("RUSTSTREAM_BENCH_SECONDS", SECONDS as usize) as f64;
    let out = env::var("RUSTSTREAM_BENCH_OUT").unwrap_or_else(|_| "bench-paired.json".to_owned());

    let runtime = runtime();
    // Outside every pair, and once: what a request the client waits for costs on this transport.
    let round_trip = runtime.block_on(round_trip(&url));
    println!(
        "round trip: {:.1} us over {ROUND_TRIPS} requests, {:.4} of them per delivery",
        round_trip.as_secs_f64() * 1e6,
        round_trips_per_delivery(),
    );
    let measured: Vec<Measured> = [Scenario::Classic, Scenario::Quorum]
        .into_iter()
        .map(|scenario| runtime.block_on(measure(scenario, &url, pairs, seconds, round_trip)))
        .collect();

    println!();
    for row in &measured {
        println!(
            "{}: raw {:.0}, adapter {:.0} ({:.1}%, {}), framework {:.0} ({:.1}%, {}) msg/s{}",
            row.scenario.name(),
            row.raw.best,
            row.adapter.best,
            row.adapter_overhead_percent,
            row.adapter_verdict,
            row.framework.best,
            row.overhead_percent,
            row.verdict,
            if row.broker_bound {
                ", broker-bound"
            } else {
                ""
            }
        );
    }

    std::fs::write(&out, document(&measured, round_trip)).expect("the summary is written");
    println!("\nwrote {out}");
}
