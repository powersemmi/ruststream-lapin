//! What the broker does with a per-message AMQP property, against a real `RabbitMQ`.
//!
//! `tests/integration_lapin.rs` proves the properties reach the frame the broker stored. These
//! prove what the server then does with them, which is the part the crate's documentation
//! promises: a priority queue hands the urgent message out first while a queue declared without
//! the argument keeps the publish order, a per-message TTL takes an unconsumed message to the
//! dead-letter queue, and the delivery mode is the one the policy and the step asked for.
//!
//! Every test is a no-op unless `AMQP_TEST_URL` points at a broker (see `just test-brokers`).

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use futures::{Stream, StreamExt};
use lapin::message::Delivery;
use lapin::options::{BasicConsumeOptions, QueueDeleteOptions};
use lapin::types::{FieldTable, ShortString};
use lapin::{Channel, Connection, ConnectionProperties, Consumer};
use ruststream::runtime::PublishExt;
use ruststream::{Broker, ConnectedBroker, IncomingMessage, Outgoing, Serialized, Subscriber};
use ruststream_lapin::{
    AMQPValue, AmqpError, LapinBroker, LapinMessage, LapinPublish, LapinPublishSteps, LapinRequest,
    PRIORITY_HEADER, RabbitQueue,
};

mod live;

const WAIT: Duration = Duration::from_secs(5);
const SILENCE: Duration = Duration::from_millis(300);

/// The broker address, or `None` to skip. Under `RUSTSTREAM_REQUIRE_LIVE` a missing address
/// fails the suite instead of skipping it.
fn amqp_url() -> Option<String> {
    live::url("AMQP_TEST_URL")
}

/// Unique per test run and per call, so runs never see each other's queues.
fn unique(base: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("ruststream-options.{base}.{}-{n}", std::process::id())
}

/// Payload bytes under a name of their own: these tests assert on what the broker did with a
/// message, so the body reaches it exactly as written, with no codec on the way.
#[derive(Outgoing, Serialized)]
struct Wire(Vec<u8>);

impl Wire {
    fn of(bytes: &[u8]) -> Self {
        Self(bytes.to_vec())
    }
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

/// Declares `def` and goes away.
///
/// The queue then exists with the descriptor's own arguments and nothing is consuming it while
/// the test fills it, which is what lets these tests ask what the QUEUE did with the messages
/// rather than what a consumer took first.
async fn declare_and_leave(url: &str, def: RabbitQueue) {
    let broker = LapinBroker::new(url.to_owned())
        .declare_topology(true)
        .connect()
        .await
        .expect("declare connect");
    let subscriber = broker.subscribe(def).await.expect("declare");
    drop(subscriber);
    broker.shutdown().await.expect("declare shutdown");
}

/// Deletes the queues a test declared: they are durable, so they outlive the connection.
async fn delete_queues(url: &str, queues: &[&str]) {
    let cleanup = Connection::connect(url, ConnectionProperties::default())
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

// The priority property orders deliveries only on a queue declared with `x-max-priority`, and
// that argument is a raw declaration argument the descriptor passes through verbatim. Both
// messages are in the queue before any consumer attaches, so the order they come out in is the
// queue's own.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_priority_queue_hands_the_urgent_message_out_first() {
    let Some(url) = amqp_url() else { return };
    let queue = unique("priority");
    declare_and_leave(
        &url,
        RabbitQueue::new(&queue).argument("x-max-priority", AMQPValue::ShortInt(9)),
    )
    .await;

    let broker = LapinBroker::new(url.clone())
        .connect()
        .await
        .expect("connect");
    // Confirms, so both messages are on the queue before the consumer opens.
    let publisher = broker.publisher(LapinPublish::default().confirms());
    publisher
        .message(&Wire::of(b"routine"))
        .to(&queue)
        .priority(1)
        .publish()
        .await
        .expect("publish the routine message first");
    publisher
        .message(&Wire::of(b"urgent"))
        .to(&queue)
        .priority(9)
        .publish()
        .await
        .expect("publish the urgent message second");

    let mut subscriber = broker
        .subscribe(RabbitQueue::new(&queue))
        .await
        .expect("subscribe");
    let mut stream = Box::pin(subscriber.stream());
    let first = next(&mut stream).await;
    assert_eq!(
        first.payload(),
        b"urgent",
        "the queue hands the higher priority out first, whatever the publish order"
    );
    first.ack().await.expect("ack");
    let second = next(&mut stream).await;
    assert_eq!(second.payload(), b"routine");
    second.ack().await.expect("ack");

    drop(stream);
    drop(subscriber);
    broker.shutdown().await.expect("shutdown");
    delete_queues(&url, &[&queue]).await;
}

// The same two publishes on a queue that was not declared for priorities: the broker carries the
// property to the consumer and does nothing else with it, so the publish order stands.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_queue_without_the_argument_keeps_the_publish_order() {
    let Some(url) = amqp_url() else { return };
    let queue = unique("unordered");
    declare_and_leave(&url, RabbitQueue::new(&queue)).await;

    let broker = LapinBroker::new(url.clone())
        .connect()
        .await
        .expect("connect");
    let publisher = broker.publisher(LapinPublish::default().confirms());
    publisher
        .message(&Wire::of(b"routine"))
        .to(&queue)
        .priority(1)
        .publish()
        .await
        .expect("publish the routine message first");
    publisher
        .message(&Wire::of(b"urgent"))
        .to(&queue)
        .priority(9)
        .publish()
        .await
        .expect("publish the urgent message second");

    let mut subscriber = broker
        .subscribe(RabbitQueue::new(&queue))
        .await
        .expect("subscribe");
    let mut stream = Box::pin(subscriber.stream());
    let first = next(&mut stream).await;
    assert_eq!(
        first.payload(),
        b"routine",
        "without `x-max-priority` the queue orders nothing"
    );
    assert_eq!(
        first.headers().get_str(PRIORITY_HEADER),
        Some("1"),
        "the delivery still reports the property it carried"
    );
    first.ack().await.expect("ack");
    let second = next(&mut stream).await;
    assert_eq!(second.payload(), b"urgent");
    assert_eq!(second.headers().get_str(PRIORITY_HEADER), Some("9"));
    second.ack().await.expect("ack");

    drop(stream);
    drop(subscriber);
    broker.shutdown().await.expect("shutdown");
    delete_queues(&url, &[&queue]).await;
}

// The per-message expiration is a deadline the server keeps: a message nobody consumed within it
// leaves the queue, and it leaves through the queue's dead-letter route rather than vanishing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unconsumed_message_expires_into_the_dead_letter_queue() {
    let Some(url) = amqp_url() else { return };
    let queue = unique("ttl");
    let dead = unique("ttl.dead");
    declare_and_leave(&url, RabbitQueue::new(&dead)).await;
    declare_and_leave(
        &url,
        RabbitQueue::new(&queue)
            .dead_letter_exchange("")
            .dead_letter_routing_key(&dead),
    )
    .await;

    let broker = LapinBroker::new(url.clone())
        .connect()
        .await
        .expect("connect");
    let mut graveyard = broker
        .subscribe(RabbitQueue::new(&dead))
        .await
        .expect("subscribe the dead-letter queue");

    // Nothing consumes the origin queue, so the deadline runs out where the message lies.
    broker
        .publisher(LapinPublish::default().confirms())
        .message(&Wire::of(b"stale"))
        .to(&queue)
        .expiration(Duration::from_millis(200))
        .publish()
        .await
        .expect("publish with a deadline");

    let mut dead_stream = Box::pin(graveyard.stream());
    let carried = next(&mut dead_stream).await;
    assert_eq!(
        carried.payload(),
        b"stale",
        "the expired message must reach the dead-letter queue"
    );
    carried.ack().await.expect("ack the dead letter");

    // And the origin queue no longer has it: expiring is what happened to it, not a redelivery.
    let mut origin = broker
        .subscribe(RabbitQueue::new(&queue))
        .await
        .expect("subscribe the origin queue");
    let mut origin_stream = Box::pin(origin.stream());
    assert!(
        tokio::time::timeout(SILENCE, origin_stream.next())
            .await
            .is_err(),
        "the expired message must not still be on the queue it was published to",
    );

    drop(origin_stream);
    drop(dead_stream);
    drop(origin);
    drop(graveyard);
    broker.shutdown().await.expect("shutdown");
    delete_queues(&url, &[&queue, &dead]).await;
}

/// A no-ack consumer straight off the wire: the delivery mode is not a header, so the frame the
/// broker stored is the only place to read it.
async fn consume_raw(channel: &Channel, queue: &str) -> Consumer {
    channel
        .basic_consume(
            queue.into(),
            ShortString::default(),
            BasicConsumeOptions {
                no_ack: true,
                ..BasicConsumeOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .expect("probe consume")
}

async fn next_raw(consumer: &mut Consumer) -> Delivery {
    tokio::time::timeout(WAIT, consumer.next())
        .await
        .expect("delivery within timeout")
        .expect("consumer has next")
        .expect("delivery ok")
}

/// Delivery mode 2 marks a message persistent, 1 transient.
fn delivery_mode(delivery: &Delivery) -> Option<u8> {
    *delivery.properties.delivery_mode()
}

// The publish policies send persistent, the request policy sends transient, and one message
// departs from either through the builder's step.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_delivery_mode_is_the_one_the_policy_and_the_step_asked_for() {
    let Some(url) = amqp_url() else { return };
    let queue = unique("persistence");
    declare_and_leave(&url, RabbitQueue::new(&queue)).await;

    let inspect = Connection::connect(&url, ConnectionProperties::default())
        .await
        .expect("inspect connect");
    let channel = inspect.create_channel().await.expect("inspect channel");
    let mut probe = consume_raw(&channel, &queue).await;

    let broker = LapinBroker::new(url.clone())
        .connect()
        .await
        .expect("connect");
    let publisher = broker.publisher(LapinPublish::default().confirms());
    publisher
        .message(&Wire::of(b"default"))
        .to(&queue)
        .publish()
        .await
        .expect("publish with the policy default");
    assert_eq!(
        delivery_mode(&next_raw(&mut probe).await),
        Some(2),
        "a publish policy sends persistent unless told otherwise"
    );

    publisher
        .message(&Wire::of(b"stepped"))
        .to(&queue)
        .persistent(false)
        .publish()
        .await
        .expect("publish with the step");
    assert_eq!(
        delivery_mode(&next_raw(&mut probe).await),
        Some(1),
        "the step must depart from the policy for this one message"
    );

    let transient = broker.publisher(LapinPublish::default().persistent(false).confirms());
    transient
        .message(&Wire::of(b"policy"))
        .to(&queue)
        .publish()
        .await
        .expect("publish through the transient policy");
    assert_eq!(delivery_mode(&next_raw(&mut probe).await), Some(1));

    // A request nobody is waiting for after the timeout gains nothing from surviving a restart,
    // so this is the one policy whose default is the other way round.
    let requester = broker.requester(LapinRequest::default());
    requester
        .message(&Wire::of(b"request"))
        .to(&queue)
        .publish()
        .await
        .expect("publish through the requester");
    assert_eq!(
        delivery_mode(&next_raw(&mut probe).await),
        Some(1),
        "requests are transient by default"
    );

    let durable_requests = broker.requester(LapinRequest::default().persistent(true));
    durable_requests
        .message(&Wire::of(b"durable-request"))
        .to(&queue)
        .publish()
        .await
        .expect("publish through the persistent requester");
    assert_eq!(delivery_mode(&next_raw(&mut probe).await), Some(2));

    broker.shutdown().await.expect("shutdown");
    inspect
        .close(200, "OK".into())
        .await
        .expect("inspect close");
    delete_queues(&url, &[&queue]).await;
}
