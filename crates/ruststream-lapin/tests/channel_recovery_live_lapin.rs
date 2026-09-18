//! What becomes of a live handle when the broker closes the channel under it.
//!
//! AMQP answers a bad publish by closing the channel it arrived on and leaving the connection up:
//! an exchange that does not exist, or one deleted while a service was publishing to it, is
//! enough. Every handle in this crate keeps a channel for its lifetime, so the question each test
//! here asks is whether the handle is finished after that answer or opens another one. The
//! failure it must not hide is the opposite one: a publish the broker refused has to be reported
//! where the publisher awaits a confirm.
//!
//! Every test is a no-op unless `AMQP_TEST_URL` points at a broker (see `just test-brokers`).

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use futures::{Stream, StreamExt};
use lapin::options::{
    ExchangeDeclareOptions, ExchangeDeleteOptions, QueueBindOptions, QueueDeclareOptions,
    QueueDeleteOptions,
};
use lapin::types::FieldTable;
use lapin::{Connection, ConnectionProperties, ExchangeKind};
use ruststream::{
    Broker, ConnectedBroker, IncomingMessage, OutgoingMessage, Publisher, RequestReply, Subscriber,
};
use ruststream_lapin::{
    AmqpError, LapinBroker, LapinMessage, LapinPublish, LapinRequest, RabbitExchange, RabbitQueue,
};

mod live;

const WAIT: Duration = Duration::from_secs(5);
const SILENCE: Duration = Duration::from_millis(300);

/// The broker address, or `None` to skip. Under `RUSTSTREAM_REQUIRE_LIVE` a missing address
/// fails the suite instead of skipping it.
fn amqp_url() -> Option<String> {
    live::url("AMQP_TEST_URL")
}

/// Unique per test run and per call, so runs never see each other's topology.
fn unique(base: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("ruststream-recovery.{base}.{}-{n}", std::process::id())
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

/// Deletes `exchange` from a connection of its own, which is what takes its bindings with it.
async fn delete_exchange(url: &str, exchange: &str) {
    let operator = Connection::connect(url, ConnectionProperties::default())
        .await
        .expect("operator connect");
    let channel = operator.create_channel().await.expect("operator channel");
    channel
        .exchange_delete(exchange.into(), ExchangeDeleteOptions::default())
        .await
        .expect("exchange delete");
    operator
        .close(200, "OK".into())
        .await
        .expect("operator close");
}

/// Declares `exchange` again and binds `queue` to it under `key`, the way a redeploy restores the
/// topology it removed.
async fn declare_exchange_and_bind(url: &str, exchange: &str, queue: &str, key: &str) {
    let operator = Connection::connect(url, ConnectionProperties::default())
        .await
        .expect("operator connect");
    let channel = operator.create_channel().await.expect("operator channel");
    channel
        .exchange_declare(
            exchange.into(),
            ExchangeKind::Direct,
            ExchangeDeclareOptions {
                durable: true,
                ..ExchangeDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .expect("exchange declare");
    channel
        .queue_bind(
            queue.into(),
            exchange.into(),
            key.into(),
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("queue bind");
    operator
        .close(200, "OK".into())
        .await
        .expect("operator close");
}

/// Declares the durable queue a test fills, removing any leftover from an aborted run.
async fn declare_queue(url: &str, queue: &str) {
    let operator = Connection::connect(url, ConnectionProperties::default())
        .await
        .expect("operator connect");
    let channel = operator.create_channel().await.expect("operator channel");
    let _ = channel
        .queue_delete(queue.into(), QueueDeleteOptions::default())
        .await;
    channel
        .queue_declare(
            queue.into(),
            QueueDeclareOptions {
                durable: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .expect("queue declare");
    operator
        .close(200, "OK".into())
        .await
        .expect("operator close");
}

/// Removes what a test left behind: the queue is durable, so it outlives the connection.
async fn clean_up(url: &str, exchange: &str, queue: &str) {
    let operator = Connection::connect(url, ConnectionProperties::default())
        .await
        .expect("operator connect");
    let channel = operator.create_channel().await.expect("operator channel");
    let _ = channel
        .exchange_delete(exchange.into(), ExchangeDeleteOptions::default())
        .await;
    let _ = channel
        .queue_delete(queue.into(), QueueDeleteOptions::default())
        .await;
    operator
        .close(200, "OK".into())
        .await
        .expect("operator close");
}

// The shared channel every fire-and-forget publisher on a connection sends through. One publish
// to an exchange nobody declared closes it, and the publisher never sees that answer: the message
// is gone. What must not follow is a connection that has stopped publishing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_publish_to_a_missing_exchange_does_not_end_publishing_on_the_connection() {
    let Some(url) = amqp_url() else { return };
    let broker = LapinBroker::new(url)
        .declare_topology(true)
        .connect()
        .await
        .expect("connect");

    let queue = unique("shared-channel");
    let mut subscriber = broker
        .subscribe(RabbitQueue::new(&queue).durable(false).exclusive(true))
        .await
        .expect("subscribe");

    let missing = unique("no-such-exchange");
    broker
        .publisher(LapinPublish::default().exchange(&missing))
        .publish(OutgoingMessage::new(&queue, b"lost"), None)
        .await
        .expect("a publish without confirms reports what the channel took, not what came back");

    // The message went nowhere, and the window it is missed in is the window the broker's answer
    // arrives in.
    let mut stream = Box::pin(subscriber.stream());
    assert!(
        tokio::time::timeout(SILENCE, stream.next()).await.is_err(),
        "an exchange that does not exist routes nowhere",
    );

    broker
        .publisher(LapinPublish::default())
        .publish(OutgoingMessage::new(&queue, b"after"), None)
        .await
        .expect("the connection must still publish after a channel error");
    let msg = next(&mut stream).await;
    assert_eq!(
        msg.payload(),
        b"after",
        "one mistyped exchange must not cost the connection every later message"
    );
    msg.ack().await.expect("ack");

    drop(stream);
    broker.shutdown().await.expect("shutdown");
}

// The same answer where the publisher awaits a confirm: the refused publish is reported to its
// caller, and the handle publishes again once the topology is back. An exchange removed and
// restored under a running service is what a redeploy does.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_confirm_does_not_end_the_publishers_channel() {
    let Some(url) = amqp_url() else { return };
    let exchange = unique("recreated");
    let queue = unique("confirmed");
    declare_queue(&url, &queue).await;

    let broker = LapinBroker::new(url.clone())
        .declare_topology(true)
        .connect()
        .await
        .expect("connect");
    let mut subscriber = broker
        .subscribe(RabbitQueue::new(&queue).bind(RabbitExchange::direct(&exchange), "key"))
        .await
        .expect("subscribe declares the exchange and the binding");

    let publisher = broker.publisher(LapinPublish::default().exchange(&exchange).confirms());
    publisher
        .publish(OutgoingMessage::new("key", b"one"), None)
        .await
        .expect("publish");
    let mut stream = Box::pin(subscriber.stream());
    let msg = next(&mut stream).await;
    assert_eq!(msg.payload(), b"one");
    msg.ack().await.expect("ack");

    // The exchange goes away under the publisher, so the next publish has nowhere to arrive.
    delete_exchange(&url, &exchange).await;
    let refused = publisher
        .publish(OutgoingMessage::new("key", b"two"), None)
        .await
        .expect_err("a confirms publish must report an exchange that is no longer there");
    assert!(
        matches!(refused, AmqpError::Publish(_)),
        "the refusal must name the publish, got {refused:?}"
    );

    // Declared again, and the same handle has to send through it.
    declare_exchange_and_bind(&url, &exchange, &queue, "key").await;
    publisher
        .publish(OutgoingMessage::new("key", b"three"), None)
        .await
        .expect("the publisher must open another channel rather than stay closed for good");
    let msg = next(&mut stream).await;
    assert_eq!(msg.payload(), b"three");
    msg.ack().await.expect("ack");

    drop(stream);
    drop(subscriber);
    broker.shutdown().await.expect("shutdown");
    clean_up(&url, &exchange, &queue).await;
}

// The requester's channel carries its reply consumer, so losing it loses the replies in flight -
// that is what the per-request timeout is for. What it must not lose is the handle: the next call
// opens another channel.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_requester_whose_channel_the_broker_closed_opens_another() {
    let Some(url) = amqp_url() else { return };
    let broker = LapinBroker::new(url).connect().await.expect("connect");

    // Nobody declared this exchange, so the broker answers the request's publish by closing the
    // channel, and the timeout is many round trips longer than that answer takes.
    let missing = unique("no-such-exchange");
    let requester = broker.requester(LapinRequest::default().exchange(&missing));
    let unanswered = requester
        .request(OutgoingMessage::new("anything", b"{}"), SILENCE)
        .await
        .expect_err("a request through an exchange that does not exist cannot be answered");
    assert!(
        matches!(unanswered, AmqpError::RequestTimeout(_)),
        "no reply within the deadline is a timeout, got {unanswered:?}"
    );

    requester
        .publish(OutgoingMessage::new("anything", b"{}"), None)
        .await
        .expect("the requester must open another channel rather than stay closed for good");

    broker.shutdown().await.expect("shutdown");
}
