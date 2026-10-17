//! The native delay backend over infrastructure this service does not declare, against a real
//! `RabbitMQ`.
//!
//! A delayed retry re-publishes the delivery to its waiting queue (or delayed exchange) and acks
//! the original. Where the target does not exist, the server drops the copy (a waiting queue on
//! the default exchange) or closes the channel (an exchange), so the subscription checks for it
//! once, when it opens. Every test is a no-op unless `AMQP_TEST_URL` points at a broker.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use futures::StreamExt;
use lapin::options::{QueueDeclareOptions, QueueDeleteOptions};
use lapin::types::{AMQPValue, FieldTable, ShortString};
use ruststream::{
    Broker, ConnectedBroker, IncomingMessage, OutgoingMessage, Publisher, Subscriber,
};
use ruststream_lapin::prelude::*;
use ruststream_lapin::{ConnectedLapinBroker, LapinPublish};

mod live;

const WAIT: Duration = Duration::from_secs(5);

fn amqp_url() -> Option<String> {
    live::url("AMQP_TEST_URL")
}

/// Unique per test run and per call, so runs never see each other's queues.
fn unique(base: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("ruststream-delay.{base}.{}-{n}", std::process::id())
}

/// Provisions `queue` the way an operator does, outside the service, with `arguments`.
async fn provision(url: &str, queue: &str, arguments: FieldTable) {
    let operator = lapin::Connection::connect(url, lapin::ConnectionProperties::default())
        .await
        .expect("operator connect");
    let channel = operator.create_channel().await.expect("operator channel");
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
        .expect("operator declares the queue");
    operator
        .close(200, "OK".into())
        .await
        .expect("operator close");
}

/// Deletes the queues a test provisioned: they are durable, so they outlive every connection.
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

/// A broker that finds its topology instead of declaring it.
async fn connect(url: &str) -> ConnectedLapinBroker {
    LapinBroker::new(url.to_owned())
        .declare_topology(false)
        .connect()
        .await
        .expect("connect")
}

// The server drops a copy published to a waiting queue that does not exist, and the original is
// acked after it, so the subscription refuses to open rather than lose every delayed message.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_delayed_retry_with_no_waiting_queue_refuses_to_start() {
    let Some(url) = amqp_url() else { return };
    let queue = unique("no-waiting-queue");
    let waiting = format!("{queue}.retry");
    provision(&url, &queue, FieldTable::default()).await;
    let connected = connect(&url).await;

    let err = connected
        .subscribe(RabbitQueue::new(&queue).delay(Delay::dlx_ttl()))
        .await
        .expect_err("a delayed copy would have nowhere to wait");
    let message = err.to_string();
    assert!(message.contains(&format!("{queue:?}")), "{message}");
    assert!(message.contains(&format!("{waiting:?}")), "{message}");
    assert!(message.contains("declare_topology(true)"), "{message}");

    connected.shutdown().await.expect("shutdown");
    delete_queues(&url, &[&queue]).await;
}

// A waiting queue the operator provisioned is found, and a delayed copy comes back through it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_provisioned_waiting_queue_carries_a_delayed_retry() {
    let Some(url) = amqp_url() else { return };
    let queue = unique("provisioned");
    let waiting = format!("{queue}.retry");
    provision(&url, &queue, FieldTable::default()).await;
    let mut back_to_origin = FieldTable::default();
    back_to_origin.insert(
        ShortString::from("x-dead-letter-exchange"),
        AMQPValue::LongString(String::new().into()),
    );
    back_to_origin.insert(
        ShortString::from("x-dead-letter-routing-key"),
        AMQPValue::LongString(queue.as_str().into()),
    );
    provision(&url, &waiting, back_to_origin).await;
    let connected = connect(&url).await;

    let mut subscriber = connected
        .subscribe(RabbitQueue::new(&queue).delay(Delay::dlx_ttl()))
        .await
        .expect("the waiting queue exists");
    connected
        .publisher(LapinPublish::default())
        .publish(OutgoingMessage::new(&queue, b"later".as_slice()), None)
        .await
        .expect("publish");

    let mut stream = Box::pin(subscriber.stream());
    let first = tokio::time::timeout(WAIT, stream.next())
        .await
        .expect("delivery within timeout")
        .expect("stream has next")
        .expect("delivery ok");
    first
        .nack_after(Duration::from_millis(150))
        .await
        .expect("the waiting queue takes the copy");
    let again = tokio::time::timeout(WAIT, stream.next())
        .await
        .expect("the copy comes back within timeout")
        .expect("stream has next")
        .expect("delivery ok");
    assert_eq!(again.payload(), b"later");
    again.ack().await.expect("ack");

    drop(stream);
    drop(subscriber);
    connected.shutdown().await.expect("shutdown");
    delete_queues(&url, &[&queue, &waiting]).await;
}

// A copy published to a delayed exchange that does not exist closes the subscription's channel,
// so the subscription refuses to open rather than stop at its first delayed retry.
#[cfg(feature = "plugin-dme")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_delayed_retry_with_no_delayed_exchange_refuses_to_start() {
    let Some(url) = amqp_url() else { return };
    let queue = unique("no-delayed-exchange");
    let exchange = format!("{queue}.delay");
    provision(&url, &queue, FieldTable::default()).await;
    let connected = connect(&url).await;

    let err = connected
        .subscribe(RabbitQueue::new(&queue).delay(Delay::plugin_dme()))
        .await
        .expect_err("a delayed copy would close the channel");
    let message = err.to_string();
    assert!(message.contains(&format!("{queue:?}")), "{message}");
    assert!(message.contains(&format!("{exchange:?}")), "{message}");
    assert!(message.contains("declare_topology(true)"), "{message}");

    connected.shutdown().await.expect("shutdown");
    delete_queues(&url, &[&queue]).await;
}
