//! Shared parts of the crate's code-cost benchmarks: what is measured, and how the measurement is
//! kept to one region.
//!
//! # What a scenario looks like
//!
//! One scenario per file, and this module carries what they have in common: the payload, the
//! service setup, the latch a handler counts deliveries down on, and the measurement
//! configuration. The method is the core's, described in its `benches/common` and on the
//! [RustStream benchmarks page](https://powersemmi.github.io/ruststream/latest/benchmarks/).
//!
//! A scenario runs the service a user writes: the app, started through [`RustStream::start`], on
//! [`LapinBroker`], the production broker, connected to the `RabbitMQ` node of the repository's
//! compose stand. The subscription consumes a classic queue the service declares at start, with
//! the prefetch window the comparison on the benchmarks page uses. The service runs on a
//! single-threaded tokio runtime.
//!
//! # Steady state and cold start
//!
//! Every scenario is measured over one delivery, over [`MESSAGES`] deliveries and over twice as
//! many. The slope between the last two is the steady-state cost of a message: everything that
//! happens once is in both totals and cancels in the subtraction. The one-delivery run is the
//! cold start, reported on its own: connecting, opening the channels, declaring the queue and
//! registering the consumer, then the first delivery.
//!
//! What a body measures is the start and the drain, in two regions. The queue is filled between
//! them from another thread with a runtime and a connection of its own, and the fill waits for the
//! node to confirm every message. The service's runtime does not run outside a region, so nothing
//! is handled while the queue fills: the drain takes every delivery.
//!
//! # What is counted
//!
//! Collection starts switched off and is switched on for [`measure`], which every body wraps its
//! work in. What is counted is everything on the service's thread inside the region: the
//! dispatcher, the codec, this crate's code, and the `lapin` client's work on that thread,
//! tokio's share of driving them included. `lapin` reads and writes the socket on a thread of its
//! own, and that thread is not counted; neither is the fill. [`measure`] is the only frame that
//! carries its name, because a toggle on a name that also appears inside closure types switches
//! collection off again one frame deeper. DHAT is pointed at the same frame; the number read is
//! `Total blocks`, allocations per run.
//!
//! A real node answers in its own time, so how often the service's thread waits for the socket
//! differs a little between runs, and so does the instruction count.

// Each benchmark target compiles this module on its own and uses the part it needs; what another
// target uses looks unused here.
#![allow(dead_code)]

use std::convert::Infallible;
use std::env;
use std::future::Future;
use std::hint::black_box;
use std::num::NonZeroU16;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

use gungraun::{Callgrind, Dhat, DhatMetric, EntryPoint, EventKind, LibraryBenchmarkConfig};
use lapin::options::{BasicPublishOptions, ConfirmSelectOptions, QueueDeleteOptions};
use lapin::types::ShortString;
use lapin::{BasicProperties, Connection, ConnectionProperties};
use ruststream::nonzero;
use ruststream::runtime::{AppInfo, BrokerScope, Identity, RunningApp, RustStream};
use ruststream_lapin::LapinBroker;
use serde::Deserialize;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::Notify;

// A benchmark measures what ships, and the framework's harness feature changes the dispatch path.
#[cfg(feature = "testing")]
compile_error!(
    "benchmarks must be built without the `testing` feature; run them through `just bench-code`"
);

/// The queue every scenario delivers on: a routing key on the default exchange names it. A handler
/// names it in its own `#[subscriber(..)]` attribute, which takes a literal.
pub const INPUT: &str = "orders";

/// The node the compose stand publishes, unless `AMQP_TEST_URL` names another one.
const DEFAULT_URL: &str = "amqp://127.0.0.1:5672";

/// Unacknowledged deliveries the node pushes before it waits: the window the comparison on the
/// benchmarks page uses.
const PREFETCH: NonZeroU16 = nonzero!(512);

/// The values every body carries. Fixed, so that every delivery of a run costs the same.
const ID: u64 = 1_000_000;
const QUANTITY: u32 = 37;

/// The payload every scenario decodes: two integer fields, so a decode allocates nothing and the
/// number is about the crate and the framework rather than about `serde_json`'s string handling.
#[derive(Debug, Deserialize)]
pub struct Order {
    pub id: u64,
    pub quantity: u32,
}

/// Deliveries per measured run: large enough that entering and leaving the region is lost in the
/// per-message number, small enough that a scenario stays within a minute of valgrind time.
/// `scripts/bench_results.py` divides by the same count.
pub const MESSAGES: usize = 1_000;

/// The measurement configuration every gated scenario shares.
///
/// `steady` is what one delivery allocates in the steady state and `cold` what starting the
/// service and taking the first delivery allocate once; together they are the hard limit the
/// longest run of the scenario (twice [`MESSAGES`] deliveries) is held to, so the run
/// fails when the path allocates more than it does today. Against a real node a count moves by a
/// block or so between runs, so each scenario's floor is the highest count its runs produced plus
/// a tenth of a percent, which one extra allocation per delivery still exceeds. Both are floors the
/// code is held to, so a number that goes down is lowered here in the same change. The
/// instruction limit is relative, [`INSTRUCTION_LIMIT`] percent over the run compared against:
/// `just bench-code --save-baseline=main` records a baseline and `just bench-code
/// --baseline=main` compares against it.
pub fn config(steady: u64, cold: u64) -> LibraryBenchmarkConfig {
    config_every(steady, 1, cold)
}

/// The same for a scenario whose allocations do not come one per delivery: `steady` blocks per
/// `per` deliveries, as a batch handler allocates per batch.
pub fn config_every(steady: u64, per: u64, cold: u64) -> LibraryBenchmarkConfig {
    let mut config = LibraryBenchmarkConfig::default();
    config
        .pass_through_env("AMQP_TEST_URL")
        .tool(callgrind().soft_limits([(EventKind::Ir, INSTRUCTION_LIMIT)]))
        .tool(dhat().hard_limits([(DhatMetric::TotalBlocks, blocks(steady, per, cold))]));
    config
}

/// How many percent more instructions than the run compared against fail a scenario.
///
/// Twice what a real node moved a run's total by: over seven runs of an unchanged tree the longest
/// consume run came out 2.4 percent apart once, as the service's thread waited for the socket less
/// often, and the core's two percent would fail such a run.
const INSTRUCTION_LIMIT: f64 = 5.0;

/// The limit for the configured count: the cold part once, plus the steady rate over the longest
/// run of the scenario, which is twice [`MESSAGES`]. The division rounds up.
const fn blocks(steady: u64, per: u64, cold: u64) -> u64 {
    cold + (steady * 2 * MESSAGES as u64).div_ceil(per)
}

/// Callgrind collecting inside the measured region alone.
fn callgrind() -> Callgrind {
    let mut callgrind = Callgrind::with_args([
        "--collect-atstart=no",
        &format!("--toggle-collect={REGION}"),
    ]);
    callgrind.entry_point(EntryPoint::None);
    callgrind
}

/// The measured region: everything this runs is counted, nothing around it is.
#[inline(never)]
pub fn measure<T>(body: impl FnOnce() -> T) -> T {
    body()
}

/// DHAT with a stack window deep enough to reach the measured frame from a publish inside a
/// dispatched handler.
fn dhat() -> Dhat {
    let mut dhat = Dhat::with_args(["--num-callers=128"]);
    dhat.entry_point(EntryPoint::Custom(REGION.to_owned()));
    dhat
}

/// The frame both tools are pointed at.
const REGION: &str = "*common::measure*";

/// A single-threaded runtime: the service runs only while a body drives it, and on one thread.
pub fn runtime() -> Runtime {
    Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current-thread runtime")
}

/// The node every scenario connects to.
fn url() -> String {
    env::var("AMQP_TEST_URL").unwrap_or_else(|_| DEFAULT_URL.to_owned())
}

/// Counts deliveries down and wakes the benchmark body when the last one has been handled.
///
/// Handlers reach it as the application state. What a delivery pays for it is one relaxed
/// decrement and the branch that reads it.
#[derive(Clone, Debug)]
pub struct Latch(Arc<Inner>);

#[derive(Debug)]
struct Inner {
    remaining: AtomicUsize,
    drained: Notify,
}

impl Default for Latch {
    fn default() -> Self {
        Self(Arc::new(Inner {
            remaining: AtomicUsize::new(0),
            drained: Notify::new(),
        }))
    }
}

impl Latch {
    /// Arms the latch for `count` deliveries.
    pub fn expect(&self, count: usize) {
        self.0.remaining.store(count, Ordering::Release);
    }

    /// Records one handled delivery, waking the waiter on the last one.
    pub fn arrived(&self) {
        if self.0.remaining.fetch_sub(1, Ordering::Relaxed) == 1 {
            self.0.drained.notify_one();
        }
    }

    /// How many deliveries the latch is still waiting for.
    pub fn remaining(&self) -> usize {
        self.0.remaining.load(Ordering::Acquire)
    }

    /// Resolves once every expected delivery has been handled.
    pub async fn drained(&self) {
        while self.0.remaining.load(Ordering::Acquire) > 0 {
            self.0.drained.notified().await;
        }
    }
}

/// The JSON body every delivery carries: the two fields a handler reads.
pub fn json_body() -> Vec<u8> {
    format!("{{\"id\":{ID},\"quantity\":{QUANTITY}}}").into_bytes()
}

/// Runs `work` to completion on a thread and a runtime of its own, outside every measured region.
///
/// The service's runtime runs only inside a body's `block_on`, so anything driven on it between
/// the regions would wait for the drain. The setup and the fill go here instead.
fn aside<Work, Fut>(work: Work)
where
    Work: FnOnce(String) -> Fut + Send + 'static,
    Fut: Future<Output = ()>,
{
    let url = url();
    thread::spawn(move || runtime().block_on(work(url)))
        .join()
        .expect("the side thread finishes its work");
}

/// Connects the side thread's own client to the node.
async fn connect(url: &str) -> Connection {
    Connection::connect(url, ConnectionProperties::default())
        .await
        .expect("the RabbitMQ node accepts a connection")
}

/// Removes what an earlier run left in [`INPUT`], so every run starts on an empty queue that the
/// service declares afresh.
fn reset_queue() {
    aside(async move |url: String| {
        let connection = connect(&url).await;
        let channel = connection
            .create_channel()
            .await
            .expect("the connection opens a channel");
        channel
            .queue_delete(ShortString::from(INPUT), QueueDeleteOptions::default())
            .await
            .expect("the queue is deleted");
        connection
            .close(0, ShortString::default())
            .await
            .expect("the connection closes");
    });
}

/// Publishes `count` bodies on [`INPUT`] with the raw client and waits for the node to confirm
/// every one: when this returns the whole run is in the queue.
fn fill(count: usize) {
    aside(async move |url: String| {
        let connection = connect(&url).await;
        let channel = connection
            .create_channel()
            .await
            .expect("the connection opens a channel");
        channel
            .confirm_select(ConfirmSelectOptions::default())
            .await
            .expect("the channel turns on publisher confirms");
        let body = json_body();
        let mut confirms = Vec::with_capacity(count);
        for _ in 0..count {
            confirms.push(
                channel
                    .basic_publish(
                        ShortString::default(),
                        ShortString::from(INPUT),
                        BasicPublishOptions::default(),
                        &body,
                        BasicProperties::default(),
                    )
                    .await
                    .expect("the node accepts the publish"),
            );
        }
        for confirm in confirms {
            let confirmation = confirm.await.expect("the node confirms the publish");
            assert!(confirmation.is_ack(), "the node refused a body of the fill");
        }
        connection
            .close(0, ShortString::default())
            .await
            .expect("the connection closes");
    });
}

/// A service that is built but not started, and what its queue will hold.
///
/// The start is part of the measurement rather than of the setup, because the cold number is
/// what starting costs. It is held as a boxed call so that every scenario hands over the same
/// type; the one indirect call it adds lands in the cold number and nowhere else.
pub struct Pending {
    runtime: Runtime,
    latch: Latch,
    start: Box<dyn FnOnce(&Runtime) -> RunningApp>,
    messages: usize,
}

/// The mount a scenario passes in: what `with_broker` does with the scope.
pub type Mount<'a> = &'a mut BrokerScope<LapinBroker, Identity, (), Latch>;

/// Builds a one-handler service on the production broker, ready to be started by the body.
///
/// The broker is configured the way a service that owns its queue configures it: the prefetch
/// window, and the declaration of the queue at start.
pub fn pending(messages: usize, mount: impl FnOnce(Mount<'_>)) -> Pending {
    reset_queue();
    let latch = Latch::default();
    let broker = LapinBroker::new(url())
        .prefetch(PREFETCH)
        .declare_topology(true);
    let state = latch.clone();
    let app = RustStream::new(AppInfo::new("bench", "0.0.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(state))
        .with_broker(broker, mount);
    Pending {
        runtime: runtime(),
        latch,
        start: Box::new(move |runtime| runtime.block_on(app.start()).expect("the service starts")),
        messages,
    }
}

/// Starts the service, fills its queue, and drains it: the shape of every scenario here.
///
/// Two measured regions, and the fill between them is in neither. The first is the cold start,
/// the second the deliveries.
pub fn start_and_drain(pending: Pending) {
    let Pending {
        runtime,
        latch,
        start,
        messages,
    } = pending;
    let running = measure(|| start(&runtime));
    latch.expect(messages);
    fill(messages);
    assert_eq!(
        latch.remaining(),
        messages,
        "the queue was consumed while it was being filled, so the measured region would be short"
    );
    measure(|| runtime.block_on(latch.drained()));
    runtime
        .block_on(running.shutdown())
        .expect("the service stops");
    black_box(&latch);
}
