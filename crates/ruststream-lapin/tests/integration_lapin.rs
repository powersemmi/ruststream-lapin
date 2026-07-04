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

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use futures::{Stream, StreamExt};
use ruststream::{Broker, Headers, IncomingMessage, OutgoingMessage, Publisher, Subscriber};
use ruststream_lapin::{Delay, LapinBroker, LapinMessage, QueueType, RabbitExchange, RabbitQueue};

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
