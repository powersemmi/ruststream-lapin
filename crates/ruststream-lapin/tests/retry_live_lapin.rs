//! The retry declaration and the delivery counter against a real `RabbitMQ`.
//!
//! Both are the server's own mechanisms, so neither can be proved in process: the delivery limit
//! and the dead-letter route are queue arguments the broker acts on, and the count comes off the
//! wire. Every test is a no-op unless `AMQP_TEST_URL` points at a broker (see `just test-brokers`).
//!
//! A quorum queue counts two things as a spent delivery: one whose consumer went away without
//! settling it, and one a consumer rejected. That is why this crate settles a handler's retry with
//! `basic.reject` rather than `basic.nack`, which `RabbitMQ` 4.3 does not count, and it is what the
//! tests below spend a message's attempts with.

use std::convert::Infallible;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::{Stream, StreamExt};
use lapin::options::{QueueDeclareOptions, QueueDeleteOptions};
use lapin::types::{AMQPValue, FieldTable, ShortString};
use ruststream::runtime::RETRY_COUNT_HEADER;
use ruststream::{
    Broker, ConnectedBroker, FromRef, IncomingMessage, OutgoingMessage, Publisher,
    RetryDeclaration, Subscriber, SubscriptionSource, nonzero,
};
use ruststream_lapin::prelude::*;
use ruststream_lapin::{
    AmqpError, ConnectedLapinBroker, LapinMessage, LapinPublish, RabbitQuorumQueue,
};
use serde::Deserialize;
use tokio::sync::Notify;

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
fn declared(queue: &str, dead: &str) -> RabbitQuorumQueue {
    let declaration = RetryDeclaration::new()
        .with_max_attempts(nonzero!(ATTEMPTS))
        .with_dead_letter(dead.to_owned());
    SubscriptionSource::<ConnectedLapinBroker>::declare_retry(
        RabbitQuorumQueue::new(queue),
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

    let check = lapin::Connection::connect(&url, lapin::ConnectionProperties::default())
        .await
        .expect("check connect");
    let channel = check.create_channel().await.expect("check channel");
    // One less than the cap: the argument counts the returns a message survives, and the cap
    // counts the deliveries it gets.
    redeclare(
        &channel,
        &queue,
        quorum_arguments(&dead, i64::from(ATTEMPTS) - 1),
    )
    .await
    .expect("the queue must carry exactly the arguments the declaration asked for");
    check.close(200, "OK".into()).await.expect("check close");

    drop(subscriber);
    connected.shutdown().await.expect("shutdown");
    delete_queues(&url, &[&queue]).await;
}

/// The arguments a quorum queue carries when it dead-letters to `dead` after `returns` returns.
fn quorum_arguments(dead: &str, returns: i64) -> FieldTable {
    let mut arguments = FieldTable::default();
    arguments.insert(
        ShortString::from("x-queue-type"),
        AMQPValue::LongString("quorum".into()),
    );
    arguments.insert(
        ShortString::from("x-delivery-limit"),
        AMQPValue::LongLongInt(returns),
    );
    arguments.insert(
        ShortString::from("x-dead-letter-exchange"),
        AMQPValue::LongString(String::new().into()),
    );
    arguments.insert(
        ShortString::from("x-dead-letter-routing-key"),
        AMQPValue::LongString(dead.to_owned().into()),
    );
    arguments
}

/// Declares `queue` again with `arguments`: the server holds a redeclaration to what the queue
/// already carries, so this succeeds only if the arguments are the queue's own.
///
/// A refused declaration closes the channel it ran on, so a test that asserts both answers needs
/// a channel per answer.
async fn redeclare(
    channel: &lapin::Channel,
    queue: &str,
    arguments: FieldTable,
) -> Result<(), lapin::Error> {
    channel
        .queue_declare(
            queue.into(),
            QueueDeclareOptions {
                durable: true,
                ..QueueDeclareOptions::default()
            },
            arguments,
        )
        .await
        .map(drop)
}

// `delivery_limit` is the queue's own policy, for a queue whose retries are not one
// registration's business. The argument counts the returns a message survives, so one return is
// two deliveries, and the queue's dead-letter route is where the spent message goes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_queues_own_delivery_limit_spends_one_delivery_more_than_it_counts() {
    let Some(url) = amqp_url() else { return };
    let queue = unique("limited");
    let dead = unique("limited.dead");
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
        .subscribe(
            RabbitQuorumQueue::new(&queue)
                .delivery_limit(1)
                .dead_letter_exchange("")
                .dead_letter_routing_key(&dead),
        )
        .await
        .expect("subscribe declares the queue");

    publish(&connected, &queue, b"twice").await;

    let mut stream = Box::pin(subscriber.stream());
    for attempt in 1..=2u64 {
        let delivery = next(&mut stream).await;
        assert_eq!(delivery.payload(), b"twice", "attempt {attempt}");
        delivery.nack(true).await.expect("the handler asks again");
    }
    assert!(
        tokio::time::timeout(SILENCE, stream.next()).await.is_err(),
        "one return is two deliveries, so there must be no third",
    );

    let mut dead_stream = Box::pin(graveyard.stream());
    let carried = next(&mut dead_stream).await;
    assert_eq!(carried.payload(), b"twice");
    carried.ack().await.expect("ack");

    drop(dead_stream);
    drop(stream);
    drop(subscriber);
    drop(graveyard);
    connected.shutdown().await.expect("shutdown");
    delete_queues(&url, &[&queue, &dead]).await;
}

/// How many messages `queue` holds, as the server counts them.
async fn ready_messages(url: &str, queue: &str) -> u32 {
    let probe = lapin::Connection::connect(url, lapin::ConnectionProperties::default())
        .await
        .expect("probe connect");
    let channel = probe.create_channel().await.expect("probe channel");
    let state = channel
        .queue_declare(
            queue.into(),
            QueueDeclareOptions {
                passive: true,
                durable: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .expect("passive declare");
    let ready = state.message_count();
    probe.close(200, "OK".into()).await.expect("probe close");
    ready
}

// A limit with no dead-letter route drops the spent message instead of carrying it away, which
// is why a mount site that declares a cap has to name a destination with it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_delivery_limit_with_no_dead_letter_route_drops_the_spent_message() {
    let Some(url) = amqp_url() else { return };
    let queue = unique("dropped");
    let connected = LapinBroker::new(url.clone())
        .declare_topology(true)
        .connect()
        .await
        .expect("connect");

    let mut subscriber = connected
        .subscribe(RabbitQuorumQueue::new(&queue).delivery_limit(1))
        .await
        .expect("subscribe declares the queue");
    publish(&connected, &queue, b"lost").await;

    let mut stream = Box::pin(subscriber.stream());
    for attempt in 1..=2u64 {
        let delivery = next(&mut stream).await;
        assert_eq!(delivery.payload(), b"lost", "attempt {attempt}");
        delivery.nack(true).await.expect("the handler asks again");
    }
    assert!(
        tokio::time::timeout(SILENCE, stream.next()).await.is_err(),
        "the deliveries are spent, so the queue must not hand the message out again",
    );

    // The consumer is gone before the count is read, so a message still in flight would show as
    // ready: an empty queue is the message being gone, not the message being held somewhere.
    drop(stream);
    drop(subscriber);
    connected.shutdown().await.expect("shutdown");
    assert_eq!(
        ready_messages(&url, &queue).await,
        0,
        "with nowhere to carry the spent message to, the queue drops it",
    );

    delete_queues(&url, &[&queue]).await;
}

// The registration is the more specific statement about the retries of the handler mounted on it,
// so its cap is written over a limit the descriptor carries.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_mount_sites_cap_is_written_over_the_descriptors_own_limit() {
    let Some(url) = amqp_url() else { return };
    let queue = unique("overridden");
    let dead = unique("overridden.dead");
    let connected = LapinBroker::new(url.clone())
        .declare_topology(true)
        .connect()
        .await
        .expect("connect");

    let declaration = RetryDeclaration::new()
        .with_max_attempts(nonzero!(ATTEMPTS))
        .with_dead_letter(dead.clone());
    let def = SubscriptionSource::<ConnectedLapinBroker>::declare_retry(
        RabbitQuorumQueue::new(&queue).delivery_limit(9),
        &declaration,
    );
    let subscriber = connected
        .subscribe(def)
        .await
        .expect("subscribe declares the queue");

    let check = lapin::Connection::connect(&url, lapin::ConnectionProperties::default())
        .await
        .expect("check connect");
    let channel = check.create_channel().await.expect("check channel");
    redeclare(
        &channel,
        &queue,
        quorum_arguments(&dead, i64::from(ATTEMPTS) - 1),
    )
    .await
    .expect("the queue must carry the cap the mount site declared");
    let contrast = check.create_channel().await.expect("check channel");
    redeclare(&contrast, &queue, quorum_arguments(&dead, 9))
        .await
        .expect_err("the descriptor's own limit must not be what the queue carries");
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
        // The worker consumes the queue somebody else declared, so it mounts the descriptor
        // without the declaration: the arguments are on the queue already, and the count the
        // queue keeps is what this consumer reads.
        let mut subscriber = worker
            .subscribe(RabbitQuorumQueue::new(&queue))
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

    let counted = || RabbitQuorumQueue::new(&queue);
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

// A handler asking for its message back is a failed attempt, and the queue has to see it as one:
// the rejection this crate sends raises the queue's own counter, so `x-delivery-limit` ends a
// retry loop even where no service is counting.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_handlers_retry_spends_a_delivery_the_queue_counts() {
    let Some(url) = amqp_url() else { return };
    let queue = unique("requeued");
    let connected = LapinBroker::new(url.clone())
        .declare_topology(true)
        .connect()
        .await
        .expect("connect");

    let mut subscriber = connected
        .subscribe(RabbitQuorumQueue::new(&queue))
        .await
        .expect("subscribe");
    publish(&connected, &queue, b"requeued").await;

    let mut stream = Box::pin(subscriber.stream());
    let first = next(&mut stream).await;
    assert_eq!(first.redelivery_count(), None);
    // What a handler's `retry()` settles with, and what the queue must count.
    first.nack(true).await.expect("requeue");

    let second = next(&mut stream).await;
    assert_eq!(
        second.redelivery_count(),
        Some(2),
        "the queue counts a rejected delivery, so the redelivery is the second"
    );
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

// What the queue carries away it also marks, but the mark is not a count: the `x-death` table on
// a dead-lettered delivery names the queues the message has left, and the dead-letter queue keeps
// a counter of its own only if it is a quorum queue.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dead_lettered_delivery_reports_no_count_of_its_own() {
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
    assert_eq!(
        carried.redelivery_count(),
        None,
        "the dead-letter queue is classic, so nothing counted this delivery"
    );
    carried.ack().await.expect("ack");

    drop(dead_stream);
    drop(stream);
    drop(subscriber);
    drop(graveyard);
    connected.shutdown().await.expect("shutdown");
    delete_queues(&url, &[&queue, &dead]).await;
}

// The whole model in one run: a handler that keeps asking for its message back spends the
// queue's deliveries one rejection at a time, and the queue carries the message away to the
// dead-letter destination when they run out, with nothing published by the service.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_retried_delivery_leaves_for_the_dead_letter_queue_at_the_cap() {
    let Some(url) = amqp_url() else { return };
    let queue = unique("retried");
    let dead = unique("retried.dead");
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
        .expect("subscribe declares the queue");

    publish(&connected, &queue, b"retried").await;

    let mut stream = Box::pin(subscriber.stream());
    for attempt in 1..=u64::from(ATTEMPTS) {
        let delivery = next(&mut stream).await;
        assert_eq!(delivery.payload(), b"retried", "attempt {attempt}");
        let counted = delivery.redelivery_count();
        if attempt == 1 {
            assert_eq!(counted, None, "the first delivery has spent nothing");
        } else {
            assert_eq!(counted, Some(attempt), "attempt {attempt}");
        }
        delivery.nack(true).await.expect("the handler asks again");
    }
    assert!(
        tokio::time::timeout(SILENCE, stream.next()).await.is_err(),
        "the deliveries are spent, so the queue must not hand the message out again",
    );

    let mut dead_stream = Box::pin(graveyard.stream());
    let carried = next(&mut dead_stream).await;
    assert_eq!(carried.payload(), b"retried");
    carried.ack().await.expect("ack");

    drop(dead_stream);
    drop(stream);
    drop(subscriber);
    drop(graveyard);
    connected.shutdown().await.expect("shutdown");
    delete_queues(&url, &[&queue, &dead]).await;
}

// Half a declaration is refused before the subscription opens: the queue carries a spent delivery
// away only when it knows both when and where, and a delivery limit on its own drops it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn half_a_declaration_refuses_to_start() {
    let Some(url) = amqp_url() else { return };
    let queue = unique("half");
    let connected = LapinBroker::new(url.clone())
        .declare_topology(true)
        .connect()
        .await
        .expect("connect");

    let declaration = RetryDeclaration::new().with_max_attempts(nonzero!(ATTEMPTS));
    let def = SubscriptionSource::<ConnectedLapinBroker>::declare_retry(
        RabbitQuorumQueue::new(&queue),
        &declaration,
    );

    let err = connected
        .subscribe(def)
        .await
        .expect_err("a cap with no destination cannot become queue arguments");
    let message = err.to_string();
    assert!(message.contains(&queue), "{message}");
    assert!(message.contains("dead_letter"), "{message}");

    connected.shutdown().await.expect("shutdown");
}

// A queue this service does not declare carries the arguments whoever declared it gave it, so a
// declaration at the mount site would promise a cap nothing applies.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_declaration_on_a_queue_this_service_does_not_declare_refuses_to_start() {
    let Some(url) = amqp_url() else { return };
    let queue = unique("undeclared");
    let dead = unique("undeclared.dead");
    let connected = LapinBroker::new(url.clone())
        .declare_topology(false)
        .connect()
        .await
        .expect("connect");

    let err = connected
        .subscribe(declared(&queue, &dead))
        .await
        .expect_err("the declaration has no queue arguments to become");
    let message = err.to_string();
    assert!(message.contains(&queue), "{message}");
    assert!(message.contains("declare_topology"), "{message}");

    connected.shutdown().await.expect("shutdown");
}

/// The queue the capped service consumes, and where a spent delivery goes. The attribute takes a
/// literal, so these two are fixed names the test cleans up after itself.
const CAPPED: &str = "ruststream-retry.classic-capped";
const CAPPED_DEAD: &str = "ruststream-retry.classic-capped.dead";

/// What each delivery of the capped queue said about the attempts spent on it, in order.
type Attempts = Arc<Mutex<Vec<Option<String>>>>;

#[derive(Clone, FromRef)]
struct Capped {
    attempts: Attempts,
    done: Arc<Notify>,
}

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

/// Never ready, and it writes down what the delivery says about its own attempts.
#[subscriber(RabbitQueue::new(CAPPED))]
async fn never_ready(
    order: &Order,
    ctx: &mut Context<'_>,
    State(attempts): State<Attempts>,
) -> HandlerOutcome {
    let _ = order.id;
    attempts
        .lock()
        .expect("attempts mutex poisoned")
        .push(ctx.headers().get_str(RETRY_COUNT_HEADER).map(str::to_owned));
    HandlerOutcome::retry()
}

/// The end of the run: the message the cap spent lands here.
#[subscriber(RabbitQueue::new(CAPPED_DEAD))]
async fn capped_out(order: &Order, State(done): State<Arc<Notify>>) -> HandlerOutcome {
    let _ = order.id;
    done.notify_one();
    HandlerOutcome::ack()
}

// A classic queue counts nothing, so the runtime counts: every immediate retry is a copy carrying
// the framework's retry count one higher, and the cap ends the loop at the dead-letter queue.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_runtimes_cap_ends_a_retry_loop_on_a_classic_queue() {
    let Some(url) = amqp_url() else { return };
    delete_queues(&url, &[CAPPED, CAPPED_DEAD]).await;

    let state = Capped {
        attempts: Attempts::default(),
        done: Arc::new(Notify::new()),
    };
    let recorded = Arc::clone(&state.attempts);
    let finished = Arc::clone(&state.done);
    let app = RustStream::new(AppInfo::new("capped", "0.1.0"))
        .on_startup(move |()| {
            let state = state;
            async move { Ok::<_, Infallible>(state) }
        })
        .with_broker(LapinBroker::new(url.clone()).declare_topology(true), |b| {
            b.include(never_ready)
                .max_attempts(nonzero!(ATTEMPTS))
                .dead_letter(CAPPED_DEAD);
            b.include(capped_out);
        });
    let running = tokio::spawn(app.run_until(async move { finished.notified().await }));

    // Published after the app is up, through a connection of its own: the queues exist by then,
    // because the subscriptions declared them.
    let publisher = LapinBroker::new(url.clone())
        .connect()
        .await
        .expect("connect");
    tokio::time::sleep(Duration::from_millis(200)).await;
    publish(&publisher, CAPPED, br#"{"id":11}"#).await;

    tokio::time::timeout(WAIT, running)
        .await
        .expect("the cap ends the loop and the app shuts down")
        .expect("app task did not panic")
        .expect("run_until succeeded");

    assert_eq!(
        *recorded.lock().expect("attempts mutex poisoned"),
        vec![None, Some("1".to_owned()), Some("2".to_owned())],
        "each copy carries the framework's count one higher",
    );

    publisher.shutdown().await.expect("shutdown");
    delete_queues(&url, &[CAPPED, CAPPED_DEAD]).await;
}

// A waiting queue releases a new message, and the server counts a new message from zero, so the
// framework's retry count is the only record of the round; the copy carries it one higher each
// time it comes back.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_copy_the_waiting_queue_releases_carries_the_count() {
    let Some(url) = amqp_url() else { return };
    let queue = unique("counted-copies");
    let waiting = format!("{queue}.retry");
    let connected = LapinBroker::new(url.clone())
        .declare_topology(true)
        .connect()
        .await
        .expect("connect");

    let mut subscriber = connected
        .subscribe(RabbitQueue::new(&queue).delay(Delay::dlx_ttl()))
        .await
        .expect("subscribe");
    publish(&connected, &queue, b"counted").await;

    let mut stream = Box::pin(subscriber.stream());
    for round in 0..=2u64 {
        let delivery = next(&mut stream).await;
        let carried = delivery.headers().get_str(RETRY_COUNT_HEADER);
        if round == 0 {
            assert_eq!(carried, None, "the first delivery has spent no attempt");
        } else {
            assert_eq!(carried, Some(round.to_string().as_str()), "round {round}");
        }
        if round == 2 {
            delivery.ack().await.expect("ack");
        } else {
            delivery
                .nack_after(Duration::from_millis(150))
                .await
                .expect("the waiting queue takes the copy");
        }
    }

    drop(stream);
    drop(subscriber);
    connected.shutdown().await.expect("shutdown");
    delete_queues(&url, &[&queue, &waiting]).await;
}

/// The queue whose delay is the broker's, the queue a copy waits in, and where a spent delivery
/// goes. The attribute takes a literal, so these are fixed names the test cleans up after itself.
const DELAYED: &str = "ruststream-retry.delayed-capped";
const DELAYED_WAIT: &str = "ruststream-retry.delayed-capped.retry";
const DELAYED_DEAD: &str = "ruststream-retry.delayed-capped.dead";

/// Short enough that three rounds fit inside the test's timeout.
const DELAY: Duration = Duration::from_millis(150);

/// Never ready, and it asks for the delay the waiting queue holds the copy for.
#[subscriber(RabbitQueue::new(DELAYED).delay(Delay::dlx_ttl()))]
async fn never_ready_later(
    order: &Order,
    ctx: &mut Context<'_>,
    State(attempts): State<Attempts>,
) -> HandlerOutcome {
    let _ = order.id;
    attempts
        .lock()
        .expect("attempts mutex poisoned")
        .push(ctx.headers().get_str(RETRY_COUNT_HEADER).map(str::to_owned));
    HandlerOutcome::retry_after(DELAY)
}

/// The end of the run: the message the cap spent lands here.
#[subscriber(RabbitQueue::new(DELAYED_DEAD))]
async fn delayed_out(order: &Order, State(done): State<Arc<Notify>>) -> HandlerOutcome {
    let _ = order.id;
    done.notify_one();
    HandlerOutcome::ack()
}

// The waiting queue releases a new message, which the server counts from zero, so the framework's
// count on the copy is the only record of the round. The runtime reads it on this path too, so the
// cap ends the loop here as it does on an immediate retry.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_runtimes_cap_ends_a_delayed_retry_loop_on_a_waiting_queue() {
    let Some(url) = amqp_url() else { return };
    delete_queues(&url, &[DELAYED, DELAYED_WAIT, DELAYED_DEAD]).await;

    let state = Capped {
        attempts: Attempts::default(),
        done: Arc::new(Notify::new()),
    };
    let recorded = Arc::clone(&state.attempts);
    let finished = Arc::clone(&state.done);
    let app = RustStream::new(AppInfo::new("delayed", "0.1.0"))
        .on_startup(move |()| {
            let state = state;
            async move { Ok::<_, Infallible>(state) }
        })
        .with_broker(LapinBroker::new(url.clone()).declare_topology(true), |b| {
            b.include(never_ready_later)
                .max_attempts(nonzero!(ATTEMPTS))
                .dead_letter(DELAYED_DEAD);
            b.include(delayed_out);
        });
    let running = tokio::spawn(app.run_until(async move { finished.notified().await }));

    // Published after the app is up, through a connection of its own: the queues exist by then,
    // because the subscriptions declared them.
    let publisher = LapinBroker::new(url.clone())
        .connect()
        .await
        .expect("connect");
    tokio::time::sleep(Duration::from_millis(200)).await;
    publish(&publisher, DELAYED, br#"{"id":12}"#).await;

    tokio::time::timeout(WAIT, running)
        .await
        .expect("the cap ends the loop and the app shuts down")
        .expect("app task did not panic")
        .expect("run_until succeeded");

    assert_eq!(
        *recorded.lock().expect("attempts mutex poisoned"),
        vec![None, Some("1".to_owned()), Some("2".to_owned())],
        "each copy the waiting queue releases carries the count one higher",
    );

    publisher.shutdown().await.expect("shutdown");
    delete_queues(&url, &[DELAYED, DELAYED_WAIT, DELAYED_DEAD]).await;
}

/// The `x-delivery-count` the server stamped, read straight off the wire.
fn delivery_count(delivery: &lapin::message::Delivery) -> Option<i64> {
    let value = delivery
        .properties
        .headers()
        .as_ref()?
        .inner()
        .get(&ShortString::from("x-delivery-count"))?;
    match value {
        AMQPValue::LongLongInt(count) => Some(*count),
        AMQPValue::LongInt(count) => Some(i64::from(*count)),
        AMQPValue::ShortInt(count) => Some(i64::from(*count)),
        AMQPValue::ShortShortInt(count) => Some(i64::from(*count)),
        _ => None,
    }
}

async fn raw_next(consumer: &mut lapin::Consumer) -> lapin::message::Delivery {
    tokio::time::timeout(WAIT, consumer.next())
        .await
        .expect("delivery within timeout")
        .expect("consumer has next")
        .expect("delivery ok")
}

// The contrast the settle frame was chosen for: the queue counts a rejected delivery and does not
// count a nacked one, so a handler's retry has to be a rejection for `x-delivery-limit` to see it.
//
// This is `RabbitMQ` 4.3's answer, and the reason the crate sends the frame it sends. Up to 4.2
// the server counted both, so a failure here is either a broker older than the stand's or a
// server that has changed its mind.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_nack_is_not_a_delivery_the_queue_counts() {
    let Some(url) = amqp_url() else { return };
    let queue = unique("nacked");
    let connected = LapinBroker::new(url.clone())
        .declare_topology(true)
        .connect()
        .await
        .expect("connect");
    let subscriber = connected
        .subscribe(RabbitQuorumQueue::new(&queue))
        .await
        .expect("subscribe declares the queue");
    drop(subscriber);
    connected.shutdown().await.expect("shutdown");

    // The two frames are the same operation on a single delivery, so only a raw client can send
    // the one this crate never sends.
    let raw = lapin::Connection::connect(&url, lapin::ConnectionProperties::default())
        .await
        .expect("raw connect");
    let channel = raw.create_channel().await.expect("raw channel");
    let mut consumer = channel
        .basic_consume(
            queue.as_str().into(),
            ShortString::default(),
            lapin::options::BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("raw consume");
    channel
        .basic_publish(
            ShortString::default(),
            queue.as_str().into(),
            lapin::options::BasicPublishOptions::default(),
            b"settled",
            lapin::BasicProperties::default(),
        )
        .await
        .expect("raw publish");

    let first = raw_next(&mut consumer).await;
    assert_eq!(delivery_count(&first), None, "nothing has been spent yet");
    channel
        .basic_nack(
            first.delivery_tag,
            lapin::options::BasicNackOptions {
                requeue: true,
                multiple: false,
            },
        )
        .await
        .expect("nack");

    let second = raw_next(&mut consumer).await;
    assert_eq!(
        delivery_count(&second),
        None,
        "a nack must not raise the counter the delivery limit reads"
    );
    channel
        .basic_reject(
            second.delivery_tag,
            lapin::options::BasicRejectOptions { requeue: true },
        )
        .await
        .expect("reject");

    let third = raw_next(&mut consumer).await;
    assert_eq!(
        delivery_count(&third),
        Some(1),
        "a rejection is the attempt the queue counts"
    );
    channel
        .basic_ack(
            third.delivery_tag,
            lapin::options::BasicAckOptions::default(),
        )
        .await
        .expect("ack");

    raw.close(200, "OK".into()).await.expect("raw close");
    delete_queues(&url, &[&queue]).await;
}
