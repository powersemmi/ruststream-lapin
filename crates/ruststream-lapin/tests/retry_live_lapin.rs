//! The retry declaration and the delivery counter against a real `RabbitMQ`.
//!
//! Both are the server's own mechanisms, so neither can be proved in process: the delivery limit
//! and the dead-letter route are queue arguments the broker acts on, and the count comes off the
//! wire. Every test is a no-op unless `AMQP_TEST_URL` points at a broker (see `just test-brokers`).
//!
//! What a quorum queue counts is a delivery that failed - one whose consumer went away without
//! settling it - so that is how the tests below spend a message's attempts. A consumer asking for
//! the message back with `basic.nack(requeue = true)` is not that, and `RabbitMQ` 4.3 does not count
//! it (4.2 did), which is why the runtime's own cap is what ends a handler-driven retry loop.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use futures::{Stream, StreamExt};
use lapin::options::{QueueDeclareOptions, QueueDeleteOptions};
use lapin::types::{AMQPValue, FieldTable, ShortString};
use ruststream::{
    Broker, ConnectedBroker, IncomingMessage, OutgoingMessage, Publisher, RetryDeclaration,
    Subscriber, SubscriptionSource, nonzero,
};
use ruststream_lapin::{
    AmqpError, ConnectedLapinBroker, LapinBroker, LapinMessage, LapinPublish, QueueType,
    RabbitQueue,
};

mod live;

const WAIT: Duration = Duration::from_secs(5);
const SILENCE: Duration = Duration::from_millis(500);

/// The cap the declaration carries, and the number of deliveries the queue must then allow.
const ATTEMPTS: u32 = 3;

fn amqp_url() -> Option<String> {
    live::url("AMQP_TEST_URL")
}

/// Unique per test run and per call, so runs never see each other's queues.
fn unique(base: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("ruststream-retry.{base}.{}-{n}", std::process::id())
}

async fn next<S>(stream: &mut S) -> LapinMessage
where
    S: Stream<Item = Result<LapinMessage, AmqpError>> + Unpin,
{
    tokio::time::timeout(WAIT, stream.next())
        .await
        .expect("delivery within timeout")
        .expect("stream has next")
        .expect("delivery ok")
}

/// Publishes one message to `queue` on the default exchange, where the routing key is its name.
async fn publish(connected: &ConnectedLapinBroker, queue: &str, payload: &[u8]) {
    connected
        .publisher(LapinPublish::default())
        .publish(OutgoingMessage::new(queue, payload), None)
        .await
        .expect("publish");
}

/// Deletes the queues a test declared: a quorum queue is durable and never auto-deletes, so it
/// outlives the connection that created it.
async fn delete_queues(url: &str, queues: &[&str]) {
    let cleanup = lapin::Connection::connect(url, lapin::ConnectionProperties::default())
        .await
        .expect("cleanup connect");
    let channel = cleanup.create_channel().await.expect("cleanup channel");
    for queue in queues {
        channel
            .queue_delete((*queue).into(), QueueDeleteOptions::default())
            .await
            .expect("cleanup queue delete");
    }
    cleanup
        .close(200, "OK".into())
        .await
        .expect("cleanup close");
}

/// A quorum queue carrying the registration's declaration, as the runtime hands it over.
fn declared(queue: &str, dead: &str) -> RabbitQueue {
    let declaration = RetryDeclaration::new()
        .with_max_attempts(nonzero!(ATTEMPTS))
        .with_dead_letter(dead.to_owned());
    SubscriptionSource::<ConnectedLapinBroker>::declare_retry(
        RabbitQueue::new(queue).queue_type(QueueType::Quorum),
        &declaration,
    )
}

// The declaration is topology on a quorum queue: what the mount site said becomes the queue's own
// delivery limit and dead-letter route. The server enforces that a redeclaration matches the
// queue's arguments, so declaring the same ones again is the assertion that they arrived.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_declaration_becomes_the_queues_own_arguments() {
    let Some(url) = amqp_url() else { return };
    let queue = unique("declared");
    let dead = unique("declared.dead");
    let connected = LapinBroker::new(url.clone())
        .declare_topology(true)
        .connect()
        .await
        .expect("connect");

    let subscriber = connected
        .subscribe(declared(&queue, &dead))
        .await
        .expect("subscribe declares the queue");

    let mut expected = FieldTable::default();
    expected.insert(
        ShortString::from("x-queue-type"),
        AMQPValue::LongString("quorum".into()),
    );
    // One less than the cap: the argument counts the returns a message survives, and the cap
    // counts the deliveries it gets.
    expected.insert(
        ShortString::from("x-delivery-limit"),
        AMQPValue::LongLongInt(i64::from(ATTEMPTS) - 1),
    );
    expected.insert(
        ShortString::from("x-dead-letter-exchange"),
        AMQPValue::LongString(String::new().into()),
    );
    expected.insert(
        ShortString::from("x-dead-letter-routing-key"),
        AMQPValue::LongString(dead.clone().into()),
    );

    let check = lapin::Connection::connect(&url, lapin::ConnectionProperties::default())
        .await
        .expect("check connect");
    let channel = check.create_channel().await.expect("check channel");
    channel
        .queue_declare(
            queue.as_str().into(),
            QueueDeclareOptions {
                durable: true,
                ..QueueDeclareOptions::default()
            },
            expected,
        )
        .await
        .expect("the queue must carry exactly the arguments the declaration asked for");
    check.close(200, "OK".into()).await.expect("check close");

    drop(subscriber);
    connected.shutdown().await.expect("shutdown");
    delete_queues(&url, &[&queue]).await;
}

// The queue itself ends the loop: a message whose consumer keeps dying is handed out
// `max_attempts` times and then leaves for the dead-letter queue, without the service publishing
// anything.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_spent_delivery_leaves_for_the_dead_letter_queue() {
    let Some(url) = amqp_url() else { return };
    let queue = unique("capped");
    let dead = unique("capped.dead");
    let connected = LapinBroker::new(url.clone())
        .declare_topology(true)
        .connect()
        .await
        .expect("connect");

    let mut graveyard = connected
        .subscribe(RabbitQueue::new(&dead))
        .await
        .expect("the dead-letter queue has to exist before a message is sent there");
    connected
        .subscribe(declared(&queue, &dead))
        .await
        .expect("subscribe declares the queue");

    publish(&connected, &queue, b"poison").await;

    for attempt in 1..=ATTEMPTS {
        // A connection of its own per attempt: closing it is what returns the delivery to the
        // queue, and the close handshake is awaited, so the next attempt cannot race the consumer
        // that dropped this one.
        let worker = LapinBroker::new(url.clone())
            .connect()
            .await
            .expect("connect");
        let mut subscriber = worker
            .subscribe(declared(&queue, &dead))
            .await
            .expect("subscribe");
        let mut stream = Box::pin(subscriber.stream());
        let delivery = next(&mut stream).await;
        assert_eq!(delivery.payload(), b"poison", "attempt {attempt}");
        // The delivery is left unsettled and the connection goes with it, which is the consumer
        // dying on a message: the queue takes it back and counts the attempt. The subscriber
        // outlives the shutdown on purpose - dropping it first starts a channel close the
        // shutdown would then race.
        drop(delivery);
        drop(stream);
        worker.shutdown().await.expect("the consumer goes away");
        drop(subscriber);
    }

    let mut spent = connected
        .subscribe(declared(&queue, &dead))
        .await
        .expect("subscribe");
    let mut stream = Box::pin(spent.stream());
    assert!(
        tokio::time::timeout(SILENCE, stream.next()).await.is_err(),
        "the attempts are spent, so the queue must not hand the message out again",
    );
    drop(stream);

    let mut dead_stream = Box::pin(graveyard.stream());
    let carried = next(&mut dead_stream).await;
    assert_eq!(carried.payload(), b"poison");
    carried.ack().await.expect("ack");

    drop(dead_stream);
    drop(spent);
    drop(graveyard);
    connected.shutdown().await.expect("shutdown");
    delete_queues(&url, &[&queue, &dead]).await;
}

// A quorum queue counts how often it has returned a message and stamps the count on the delivery;
// that count is what a registration's cap reads where the broker keeps one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_quorum_delivery_reports_how_often_it_has_come_back() {
    let Some(url) = amqp_url() else { return };
    let queue = unique("counted");
    let connected = LapinBroker::new(url.clone())
        .declare_topology(true)
        .connect()
        .await
        .expect("connect");

    let counted = || RabbitQueue::new(&queue).queue_type(QueueType::Quorum);
    let mut subscriber = connected.subscribe(counted()).await.expect("subscribe");
    publish(&connected, &queue, b"counted").await;

    let mut stream = Box::pin(subscriber.stream());
    let first = next(&mut stream).await;
    assert_eq!(first.redelivery_count(), None);
    // The consumer goes away with the delivery unsettled, which is the failure the queue counts.
    drop(first);
    drop(stream);
    drop(subscriber);

    let mut subscriber = connected.subscribe(counted()).await.expect("resubscribe");
    let mut stream = Box::pin(subscriber.stream());
    let second = next(&mut stream).await;
    assert_eq!(second.redelivery_count(), Some(2));
    second.ack().await.expect("ack");

    drop(stream);
    drop(subscriber);
    connected.shutdown().await.expect("shutdown");
    delete_queues(&url, &[&queue]).await;
}

// A classic queue counts nothing, so the delivery says so instead of guessing; a registration's
// cap then counts with the framework's own header.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_classic_delivery_reports_no_count() {
    let Some(url) = amqp_url() else { return };
    let queue = unique("uncounted");
    let connected = LapinBroker::new(url.clone())
        .declare_topology(true)
        .connect()
        .await
        .expect("connect");

    let uncounted = || RabbitQueue::new(&queue).durable(true);
    let mut subscriber = connected.subscribe(uncounted()).await.expect("subscribe");
    publish(&connected, &queue, b"uncounted").await;

    let mut stream = Box::pin(subscriber.stream());
    let first = next(&mut stream).await;
    assert_eq!(first.redelivery_count(), None);
    drop(first);
    drop(stream);
    drop(subscriber);

    let mut subscriber = connected.subscribe(uncounted()).await.expect("resubscribe");
    let mut stream = Box::pin(subscriber.stream());
    let second = next(&mut stream).await;
    // The same failure on a classic queue: it keeps no counter, so there is nothing to report.
    assert_eq!(second.redelivery_count(), None);
    second.ack().await.expect("ack");

    drop(stream);
    drop(subscriber);
    connected.shutdown().await.expect("shutdown");
    delete_queues(&url, &[&queue]).await;
}

// What the queue carries away it also marks: a rejected delivery reaches the dead-letter queue
// with an `x-death` entry naming the queue it left and why, and that entry is what tells the next
// consumer how far the message has come.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dead_lettered_delivery_reports_the_round_it_has_been_through() {
    let Some(url) = amqp_url() else { return };
    let queue = unique("rejected");
    let dead = unique("rejected.dead");
    let connected = LapinBroker::new(url.clone())
        .declare_topology(true)
        .connect()
        .await
        .expect("connect");

    let mut graveyard = connected
        .subscribe(RabbitQueue::new(&dead))
        .await
        .expect("the dead-letter queue has to exist before a message is sent there");
    let mut subscriber = connected
        .subscribe(declared(&queue, &dead))
        .await
        .expect("subscribe");

    publish(&connected, &queue, b"rejected").await;

    let mut stream = Box::pin(subscriber.stream());
    let first = next(&mut stream).await;
    assert_eq!(first.redelivery_count(), None);
    // Dropping the delivery is what sends it to the queue's dead-letter route.
    first.nack(false).await.expect("reject");

    let mut dead_stream = Box::pin(graveyard.stream());
    let carried = next(&mut dead_stream).await;
    assert_eq!(carried.payload(), b"rejected");
    assert_eq!(carried.redelivery_count(), Some(2));
    carried.ack().await.expect("ack");

    drop(dead_stream);
    drop(stream);
    drop(subscriber);
    drop(graveyard);
    connected.shutdown().await.expect("shutdown");
    delete_queues(&url, &[&queue, &dead]).await;
}
