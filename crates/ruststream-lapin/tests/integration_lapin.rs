//! Integration tests against a real `RabbitMQ`.
//!
//! Every test is a no-op unless `AMQP_TEST_URL` points at a broker:
//!
//! ```text
//! just brokers-up
//! AMQP_TEST_URL=amqp://127.0.0.1:5672 cargo test --workspace --all-features -- --test-threads=1
//! ```
//!
//! These cover exactly what the in-process test broker does not simulate: declared topology and
//! bindings, queue types, redelivery flags, dead-lettering, prefetch, and the split between the
//! plain and transactional publish paths.

use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::{Stream, StreamExt};
use serde::Deserialize;
use tokio::sync::Notify;

use ruststream::runtime::{AppInfo, HandlerResult, RustStream, State};
use ruststream::{
    Broker, FromRef, Headers, IncomingMessage, OutgoingMessage, Partitioned, Publisher, Subscriber,
    subscriber,
};
use ruststream_lapin::{
    Delay, LapinBroker, LapinMessage, PARTITION_KEY_HEADER, QueueType, RabbitExchange, RabbitQueue,
};

const WAIT: Duration = Duration::from_secs(5);
const SILENCE: Duration = Duration::from_millis(200);

fn amqp_url() -> Option<String> {
    std::env::var("AMQP_TEST_URL").ok()
}

/// Unique per test run and per call, so runs never see each other's queues.
fn unique(base: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("ruststream-it.{base}.{}-{n}", std::process::id())
}

/// A throwaway queue that vanishes with the test's connection: exclusive, because `RabbitMQ` 4
/// denies transient non-exclusive queues by default.
fn transient_queue(name: &str) -> RabbitQueue {
    RabbitQueue::new(name).durable(false).exclusive(true)
}

async fn next<S>(stream: &mut S) -> LapinMessage
where
    S: Stream<Item = Result<LapinMessage, ruststream_lapin::AmqpError>> + Unpin,
{
    tokio::time::timeout(WAIT, stream.next())
        .await
        .expect("delivery within timeout")
        .expect("stream has next")
        .expect("delivery ok")
}

async fn expect_silence<S>(stream: &mut S)
where
    S: Stream<Item = Result<LapinMessage, ruststream_lapin::AmqpError>> + Unpin,
{
    let outcome = tokio::time::timeout(SILENCE, stream.next()).await;
    assert!(outcome.is_err(), "expected no delivery, got one");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn round_trip_on_default_exchange() {
    let Some(url) = amqp_url() else { return };
    let broker = LapinBroker::new(url).declare_topology(true);
    Broker::connect(&broker).await.expect("connect");

    let queue = unique("round-trip");
    let mut subscriber = broker
        .subscribe(transient_queue(&queue))
        .await
        .expect("subscribe");

    let mut headers = Headers::new();
    headers.insert("content-type", "application/json");
    broker
        .publisher()
        .publish(OutgoingMessage::new(&queue, b"{\"id\":1}").with_headers(headers))
        .await
        .expect("publish");

    let mut stream = Box::pin(subscriber.stream());
    let msg = next(&mut stream).await;
    assert_eq!(msg.payload(), b"{\"id\":1}");
    assert_eq!(msg.headers().content_type(), Some("application/json"));
    assert_eq!(msg.routing_key(), queue);
    assert!(!msg.redelivered());
    msg.ack().await.expect("ack");

    drop(stream);
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn topic_binding_routes_by_pattern() {
    let Some(url) = amqp_url() else { return };
    let broker = LapinBroker::new(url).declare_topology(true);
    Broker::connect(&broker).await.expect("connect");

    let exchange = unique("events");
    let queue = unique("orders");
    let def = transient_queue(&queue).bind(
        RabbitExchange::topic(&exchange)
            .durable(false)
            .auto_delete(true),
        "order.*",
    );
    let mut subscriber = broker.subscribe(def).await.expect("subscribe");

    let publisher = broker.publisher().exchange(&exchange);
    publisher
        .publish(OutgoingMessage::new("order.created", b"hit"))
        .await
        .expect("publish hit");
    publisher
        .publish(OutgoingMessage::new("payment.created", b"miss"))
        .await
        .expect("publish miss");

    let mut stream = Box::pin(subscriber.stream());
    let msg = next(&mut stream).await;
    assert_eq!(msg.payload(), b"hit");
    assert_eq!(msg.exchange(), exchange);
    assert_eq!(msg.routing_key(), "order.created");
    msg.ack().await.expect("ack");

    expect_silence(&mut stream).await;

    drop(stream);
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn quorum_queue_declares_and_delivers() {
    let Some(url) = amqp_url() else { return };
    let broker = LapinBroker::new(url.clone()).declare_topology(true);
    Broker::connect(&broker).await.expect("connect");

    // Quorum queues must be durable and never auto-delete, so this one needs explicit cleanup.
    let queue = unique("quorum");
    let def = RabbitQueue::new(&queue).queue_type(QueueType::Quorum);
    let mut subscriber = broker.subscribe(def).await.expect("subscribe");

    broker
        .publisher()
        .publish(OutgoingMessage::new(&queue, b"q1"))
        .await
        .expect("publish");

    let mut stream = Box::pin(subscriber.stream());
    let msg = next(&mut stream).await;
    assert_eq!(msg.payload(), b"q1");
    msg.ack().await.expect("ack");

    drop(stream);
    drop(subscriber);
    broker.shutdown().await.expect("shutdown");

    let cleanup = lapin::Connection::connect(&url, lapin::ConnectionProperties::default())
        .await
        .expect("cleanup connect");
    let channel = cleanup.create_channel().await.expect("cleanup channel");
    channel
        .queue_delete(
            queue.as_str().into(),
            lapin::options::QueueDeleteOptions::default(),
        )
        .await
        .expect("cleanup queue delete");
    cleanup
        .close(200, "OK".into())
        .await
        .expect("cleanup close");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nack_requeue_marks_redelivered() {
    let Some(url) = amqp_url() else { return };
    let broker = LapinBroker::new(url).declare_topology(true);
    Broker::connect(&broker).await.expect("connect");

    let queue = unique("requeue");
    let mut subscriber = broker
        .subscribe(transient_queue(&queue))
        .await
        .expect("subscribe");
    broker
        .publisher()
        .publish(OutgoingMessage::new(&queue, b"again"))
        .await
        .expect("publish");

    let mut stream = Box::pin(subscriber.stream());
    let first = next(&mut stream).await;
    assert!(!first.redelivered());
    first.nack(true).await.expect("nack requeue");

    let second = next(&mut stream).await;
    assert_eq!(second.payload(), b"again");
    assert!(
        second.redelivered(),
        "requeued delivery must carry the redelivered flag"
    );
    second.ack().await.expect("ack");

    drop(stream);
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reject_dead_letters_into_dlx() {
    let Some(url) = amqp_url() else { return };
    let broker = LapinBroker::new(url).declare_topology(true);
    Broker::connect(&broker).await.expect("connect");

    let dead = unique("dead");
    let mut dead_subscriber = broker
        .subscribe(transient_queue(&dead))
        .await
        .expect("subscribe dead queue");

    // Dead-letter through the default exchange straight into the dead queue.
    let queue = unique("work");
    let def = transient_queue(&queue)
        .dead_letter_exchange("")
        .dead_letter_routing_key(&dead);
    let mut subscriber = broker.subscribe(def).await.expect("subscribe work queue");

    broker
        .publisher()
        .publish(OutgoingMessage::new(&queue, b"poison"))
        .await
        .expect("publish");

    let mut work_stream = Box::pin(subscriber.stream());
    let msg = next(&mut work_stream).await;
    msg.nack(false).await.expect("reject");

    let mut dead_stream = Box::pin(dead_subscriber.stream());
    let dead_msg = next(&mut dead_stream).await;
    assert_eq!(dead_msg.payload(), b"poison");
    dead_msg.ack().await.expect("ack dead letter");

    drop(work_stream);
    drop(dead_stream);
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn binary_header_values_round_trip() {
    let Some(url) = amqp_url() else { return };
    let broker = LapinBroker::new(url).declare_topology(true);
    Broker::connect(&broker).await.expect("connect");

    let queue = unique("binary");
    let mut subscriber = broker
        .subscribe(transient_queue(&queue))
        .await
        .expect("subscribe");

    let mut headers = Headers::new();
    headers.insert("x-blob", vec![0u8, 159, 146, 150]);
    headers.insert("x-tenant", "acme");
    broker
        .publisher()
        .publish(OutgoingMessage::new(&queue, b"payload").with_headers(headers))
        .await
        .expect("publish");

    let mut stream = Box::pin(subscriber.stream());
    let msg = next(&mut stream).await;
    assert_eq!(
        msg.headers().get("x-blob"),
        Some([0u8, 159, 146, 150].as_slice())
    );
    assert_eq!(msg.headers().get_str("x-tenant"), Some("acme"));
    msg.ack().await.expect("ack");

    drop(stream);
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prefetch_caps_unacknowledged_deliveries() {
    let Some(url) = amqp_url() else { return };
    let broker = LapinBroker::new(url).declare_topology(true);
    Broker::connect(&broker).await.expect("connect");

    let queue = unique("prefetch");
    let mut subscriber = broker
        .subscribe(transient_queue(&queue).prefetch(1))
        .await
        .expect("subscribe");

    let publisher = broker.publisher();
    for payload in [b"m1".as_slice(), b"m2", b"m3"] {
        publisher
            .publish(OutgoingMessage::new(&queue, payload))
            .await
            .expect("publish");
    }

    let mut stream = Box::pin(subscriber.stream());
    let first = next(&mut stream).await;
    assert_eq!(first.payload(), b"m1");

    // One unacknowledged delivery is the whole window: nothing else may arrive yet.
    expect_silence(&mut stream).await;

    first.ack().await.expect("ack first");
    let second = next(&mut stream).await;
    assert_eq!(second.payload(), b"m2");
    second.ack().await.expect("ack second");
    let third = next(&mut stream).await;
    third.ack().await.expect("ack third");

    drop(stream);
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_tx_publisher_is_plain_outside_a_transaction() {
    let Some(url) = amqp_url() else { return };
    let broker = LapinBroker::new(url).declare_topology(true);
    Broker::connect(&broker).await.expect("connect");

    let queue = unique("server-tx-plain");
    let mut subscriber = broker
        .subscribe(transient_queue(&queue))
        .await
        .expect("subscribe");

    let publisher = broker.publisher().server_tx();
    publisher
        .publish(OutgoingMessage::new(&queue, b"direct"))
        .await
        .expect("publish");

    let mut stream = Box::pin(subscriber.stream());
    let msg = next(&mut stream).await;
    assert_eq!(msg.payload(), b"direct");
    msg.ack().await.expect("ack");

    drop(stream);
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partition_key_round_trips_through_the_header() {
    let Some(url) = amqp_url() else { return };
    let broker = LapinBroker::new(url).declare_topology(true);
    Broker::connect(&broker).await.expect("connect");

    let queue = unique("keyed");
    let mut subscriber = broker
        .subscribe(transient_queue(&queue))
        .await
        .expect("subscribe");

    let mut headers = Headers::new();
    headers.insert(PARTITION_KEY_HEADER, "tenant-a");
    broker
        .publisher()
        .publish(OutgoingMessage::new(&queue, b"payload").with_headers(headers))
        .await
        .expect("publish");

    let mut stream = Box::pin(subscriber.stream());
    let msg = next(&mut stream).await;
    assert_eq!(
        Partitioned::partition_key(&msg),
        Some(b"tenant-a".as_slice())
    );
    msg.ack().await.expect("ack");

    drop(stream);
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nack_after_redelivers_through_the_delay_queue() {
    let Some(url) = amqp_url() else { return };
    let broker = LapinBroker::new(url.clone()).declare_topology(true);
    Broker::connect(&broker).await.expect("connect");

    let queue = unique("delayed");
    let def = transient_queue(&queue).delay(Delay::dlx_ttl());
    let mut subscriber = broker.subscribe(def).await.expect("subscribe");

    broker
        .publisher()
        .publish(OutgoingMessage::new(&queue, b"later"))
        .await
        .expect("publish");

    let mut stream = Box::pin(subscriber.stream());
    let first = next(&mut stream).await;
    assert_eq!(first.payload(), b"later");
    // Native delay: park it in the waiting queue for ~300ms, then dead-letter back here.
    first
        .nack_after(Duration::from_millis(300))
        .await
        .expect("nack_after");

    // It must not return before the TTL fires.
    let early = tokio::time::timeout(Duration::from_millis(100), stream.next()).await;
    assert!(
        early.is_err(),
        "the delayed message must not return before its TTL"
    );

    // It must return once the TTL fires (waiting queue -> DLX -> origin), redelivered.
    let second = next(&mut stream).await;
    assert_eq!(second.payload(), b"later");
    second.ack().await.expect("ack");

    drop(stream);
    drop(subscriber);
    broker.shutdown().await.expect("shutdown");

    // The waiting queue is durable, so remove it explicitly (the origin was exclusive/transient).
    let cleanup = lapin::Connection::connect(&url, lapin::ConnectionProperties::default())
        .await
        .expect("cleanup connect");
    let channel = cleanup.create_channel().await.expect("cleanup channel");
    channel
        .queue_delete(
            format!("{queue}.retry").as_str().into(),
            lapin::options::QueueDeleteOptions::default(),
        )
        .await
        .expect("cleanup queue delete");
    cleanup
        .close(200, "OK".into())
        .await
        .expect("cleanup close");
}

// Keyed worker lanes, driven through the full runtime dispatch path against a live broker: this is
// the behavioural counterpart to `partition_key_round_trips_through_the_header`, which only checks
// the header decode. It proves the routing guarantee end to end: deliveries that share a partition
// key stay on one lane (ordered, never concurrent), while distinct keys spread across lanes and run
// in parallel.
const KEYED_LANES_QUEUE: &str = "ruststream-it.keyed-lanes";
const KEYED_TENANTS: usize = 8;
const KEYED_PER_TENANT: usize = 3;
const KEYED_EXPECTED: usize = KEYED_TENANTS * KEYED_PER_TENANT;

/// Records, per partition key, what the lanes actually did so the test can assert the routing
/// contract after the app drains.
#[derive(Clone)]
struct Lanes(Arc<LanesInner>);

struct LanesInner {
    /// The ids seen for each key, in handler order: must match the publish order for that key.
    order: Mutex<HashMap<String, Vec<u64>>>,
    /// The distinct lane fingerprints (task ids) a key was handled on: must stay at one.
    workers: Mutex<HashMap<String, HashSet<String>>>,
    /// Keys currently inside the handler, to catch same-key concurrency.
    active: Mutex<HashSet<String>>,
    /// A same-key overlap ever observed (must be zero).
    violations: AtomicUsize,
    /// The peak number of distinct keys in flight at once (must exceed one: real parallelism).
    max_active: AtomicUsize,
    processed: AtomicUsize,
    done: Notify,
}

impl Lanes {
    fn new() -> Self {
        Self(Arc::new(LanesInner {
            order: Mutex::new(HashMap::new()),
            workers: Mutex::new(HashMap::new()),
            active: Mutex::new(HashSet::new()),
            violations: AtomicUsize::new(0),
            max_active: AtomicUsize::new(0),
            processed: AtomicUsize::new(0),
            done: Notify::new(),
        }))
    }

    fn enter(&self, tenant: &str, id: u64) {
        let inner = &self.0;
        // The handler is awaited inline in its lane's task, so the task id is a stable per-lane
        // fingerprint: a key that always hashes to the same lane must always report the same id.
        let worker = tokio::task::try_id().map_or_else(|| "?".to_owned(), |id| id.to_string());
        inner
            .workers
            .lock()
            .expect("workers lock")
            .entry(tenant.to_owned())
            .or_default()
            .insert(worker);
        inner
            .order
            .lock()
            .expect("order lock")
            .entry(tenant.to_owned())
            .or_default()
            .push(id);
        let mut active = inner.active.lock().expect("active lock");
        if !active.insert(tenant.to_owned()) {
            inner.violations.fetch_add(1, Ordering::Relaxed);
        }
        inner.max_active.fetch_max(active.len(), Ordering::Relaxed);
    }

    fn leave(&self, tenant: &str) {
        let inner = &self.0;
        inner.active.lock().expect("active lock").remove(tenant);
        if inner.processed.fetch_add(1, Ordering::Relaxed) + 1 == KEYED_EXPECTED {
            inner.done.notify_one();
        }
    }
}

#[derive(Deserialize)]
struct KeyedOrder {
    id: u64,
    tenant: String,
}

#[derive(FromRef)]
struct KeyedState {
    lanes: Lanes,
}

#[subscriber(RabbitQueue::new(KEYED_LANES_QUEUE), workers(4, by_key))]
async fn keyed_handler(order: &KeyedOrder, State(lanes): State<Lanes>) -> HandlerResult {
    lanes.enter(&order.tenant, order.id);
    // Widen the window so an erroneous same-key overlap would actually collide in `active`.
    tokio::time::sleep(Duration::from_millis(25)).await;
    lanes.leave(&order.tenant);
    HandlerResult::Ack
}

/// Declares the keyed-lanes queue durable and empty, deleting any leftover from an aborted run so
/// the pre-queued deliveries survive until the app consumes them. The consumer then subscribes
/// without declaring.
async fn declare_empty_keyed_queue(url: &str) {
    let setup = lapin::Connection::connect(url, lapin::ConnectionProperties::default())
        .await
        .expect("setup connect");
    let channel = setup.create_channel().await.expect("setup channel");
    let _ = channel
        .queue_delete(
            KEYED_LANES_QUEUE.into(),
            lapin::options::QueueDeleteOptions::default(),
        )
        .await;
    channel
        .queue_declare(
            KEYED_LANES_QUEUE.into(),
            lapin::options::QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            lapin::types::FieldTable::default(),
        )
        .await
        .expect("setup declare");
    setup.close(200, "OK".into()).await.expect("setup close");
}

/// Pre-queues every delivery round-robin, so each key's ids ascend while keys interleave. Publishes
/// through the broker so the partition-key header is written exactly as the consumer reads it.
async fn prequeue_keyed_deliveries(url: &str) {
    let producer = LapinBroker::new(url.to_owned());
    Broker::connect(&producer).await.expect("producer connect");
    let publisher = producer.publisher();
    for round in 0..KEYED_PER_TENANT {
        for tenant in 0..KEYED_TENANTS {
            let id = (round * KEYED_TENANTS + tenant) as u64;
            let tenant = format!("t{tenant}");
            let mut headers = Headers::new();
            headers.insert(PARTITION_KEY_HEADER, tenant.clone());
            let body = format!("{{\"id\":{id},\"tenant\":\"{tenant}\"}}");
            publisher
                .publish(
                    OutgoingMessage::new(KEYED_LANES_QUEUE, body.as_bytes()).with_headers(headers),
                )
                .await
                .expect("publish");
        }
    }
    producer.shutdown().await.expect("producer shutdown");
}

/// Sync so the lock guards never straddle an await point: asserts the full routing contract.
fn assert_keyed_routing(inner: &LanesInner) {
    assert_eq!(
        inner.processed.load(Ordering::Relaxed),
        KEYED_EXPECTED,
        "every published delivery was handled"
    );
    assert_eq!(
        inner.violations.load(Ordering::Relaxed),
        0,
        "a partition key must never be inside the handler on two lanes at once"
    );

    // Snapshot the maps and release the guards at once, so no lock is held across the asserts.
    let order = inner.order.lock().expect("order lock").clone();
    let workers = inner.workers.lock().expect("workers lock").clone();
    let mut all_lanes: HashSet<String> = HashSet::new();
    for t in 0..KEYED_TENANTS {
        let tenant = format!("t{t}");
        let seen = order
            .get(&tenant)
            .unwrap_or_else(|| panic!("{tenant} was handled"));
        let expected: Vec<u64> = (0..KEYED_PER_TENANT)
            .map(|round| (round * KEYED_TENANTS + t) as u64)
            .collect();
        assert_eq!(
            seen, &expected,
            "deliveries for {tenant} must arrive in publish order"
        );
        let lanes_for_key = workers.get(&tenant).expect("worker set");
        assert_eq!(
            lanes_for_key.len(),
            1,
            "every delivery for {tenant} must land on the same lane"
        );
        all_lanes.extend(lanes_for_key.iter().cloned());
    }
    assert!(
        all_lanes.len() > 1,
        "distinct keys must be pinned across several lanes, not funnelled into one"
    );
    assert!(
        inner.max_active.load(Ordering::Relaxed) > 1,
        "distinct keys must be handled in parallel"
    );
}

async fn delete_keyed_queue(url: &str) {
    let cleanup = lapin::Connection::connect(url, lapin::ConnectionProperties::default())
        .await
        .expect("cleanup connect");
    let channel = cleanup.create_channel().await.expect("cleanup channel");
    channel
        .queue_delete(
            KEYED_LANES_QUEUE.into(),
            lapin::options::QueueDeleteOptions::default(),
        )
        .await
        .expect("cleanup queue delete");
    cleanup
        .close(200, "OK".into())
        .await
        .expect("cleanup close");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn keyed_lanes_pin_each_partition_to_one_ordered_worker() {
    let Some(url) = amqp_url() else { return };

    declare_empty_keyed_queue(&url).await;
    prequeue_keyed_deliveries(&url).await;

    let lanes = Lanes::new();
    let state_lanes = lanes.clone();
    let signal = Arc::clone(&lanes.0);
    let broker = LapinBroker::new(url.clone()).declare_topology(false);
    let app = RustStream::new(AppInfo::new("keyed-lanes", "0.1.0"))
        .on_startup(move |()| {
            let lanes = state_lanes;
            async move { Ok::<_, Infallible>(KeyedState { lanes }) }
        })
        .with_broker(broker, |b| {
            b.include(keyed_handler);
        });

    // The app shuts itself down once every delivery has settled, bounded so a broken routing that
    // never reaches the count fails the test instead of hanging.
    let app_task = tokio::spawn(app.run_until(async move { signal.done.notified().await }));
    tokio::time::timeout(Duration::from_secs(20), app_task)
        .await
        .expect("app reached the expected count and shut down in time")
        .expect("app task did not panic")
        .expect("run_until succeeded");

    assert_keyed_routing(&lanes.0);
    delete_keyed_queue(&url).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscribe_fails_without_declaration_when_queue_is_missing() {
    let Some(url) = amqp_url() else { return };
    // No declare_topology: the framework must not create infrastructure on its own.
    let broker = LapinBroker::new(url).declare_topology(false);
    Broker::connect(&broker).await.expect("connect");

    let queue = unique("missing");
    let result = broker.subscribe(RabbitQueue::new(&queue)).await;
    assert!(
        result.is_err(),
        "consuming a nonexistent queue must fail, not create it"
    );

    broker.shutdown().await.expect("shutdown");
}
