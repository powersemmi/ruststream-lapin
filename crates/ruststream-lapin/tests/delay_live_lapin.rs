//! The native delay backend over infrastructure this service does not declare, against a real
//! `RabbitMQ`.
//!
//! A delayed retry re-publishes the delivery to its waiting queue (or delayed exchange) and acks
//! the original. Where the target does not exist, the server drops the copy (a waiting queue on
//! the default exchange) or closes the channel (an exchange), so the subscription checks for it
//! once, when it opens. Every test is a no-op unless `AMQP_TEST_URL` points at a broker.

use std::process::id;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use futures::StreamExt;
use lapin::options::{
    BasicGetOptions, BasicPublishOptions, BasicRejectOptions, ConfirmSelectOptions,
    QueueDeclareOptions, QueueDeleteOptions,
};
use lapin::types::ShortString;
use lapin::{BasicProperties, Connection, ConnectionProperties};
use ruststream::{ConnectedBroker, OutgoingMessage, Subscriber};
use ruststream_lapin::ConnectedLapinBroker;
use ruststream_lapin::prelude::*;
use tokio::time::timeout as timeout_future;

use crate::live;

const WAIT: Duration = Duration::from_secs(5);

fn amqp_url() -> Option<String> {
    live::url("AMQP_TEST_URL")
}

/// Unique per test run and per call, so runs never see each other's queues.
fn unique(base: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("ruststream-delay.{base}.{}-{n}", id())
}

/// Provisions `queue` the way an operator does, outside the service, with `arguments`.
async fn provision(url: &str, queue: &str, arguments: FieldTable) {
    let operator = Connection::connect(url, ConnectionProperties::default())
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
    let first = timeout_future(WAIT, stream.next())
        .await
        .expect("delivery within timeout")
        .expect("stream has next")
        .expect("delivery ok");
    first
        .nack_after(Duration::from_millis(150))
        .await
        .expect("the waiting queue takes the copy");
    let again = timeout_future(WAIT, stream.next())
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

/// Arguments that dead-letter what a queue rejects or expires to `queue`, on the default exchange.
fn dead_letter_to(queue: &str) -> FieldTable {
    let mut arguments = FieldTable::default();
    arguments.insert(
        ShortString::from("x-dead-letter-exchange"),
        AMQPValue::LongString(String::new().into()),
    );
    arguments.insert(
        ShortString::from("x-dead-letter-routing-key"),
        AMQPValue::LongString(queue.into()),
    );
    arguments
}

// A dead-lettered delivery carries the server's own record and a foreign producer's typed header,
// and a delayed retry re-publishes both as text. The copy still comes back through the waiting
// queue, the producer's header reads as it did, and the record is the one the server wrote for the
// wait.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dead_lettered_delivery_comes_back_from_a_delayed_retry() {
    let Some(url) = amqp_url() else { return };
    let origin = unique("dead-lettered");
    let dead = format!("{origin}.dead");
    let waiting = format!("{dead}.retry");
    provision(&url, &origin, dead_letter_to(&dead)).await;
    provision(&url, &dead, FieldTable::default()).await;
    provision(&url, &waiting, dead_letter_to(&dead)).await;

    // A foreign producer publishes a typed header, and a raw consumer rejects the message, so the
    // server moves it to the dead-letter queue with its record.
    let client = Connection::connect(&url, ConnectionProperties::default())
        .await
        .expect("client connect");
    let channel = client.create_channel().await.expect("client channel");
    channel
        .confirm_select(ConfirmSelectOptions::default())
        .await
        .expect("confirm.select");
    let mut typed = FieldTable::default();
    typed.insert(ShortString::from("x-attempt"), AMQPValue::LongInt(3));
    channel
        .basic_publish(
            ShortString::default(),
            origin.as_str().into(),
            BasicPublishOptions::default(),
            b"later",
            BasicProperties::default().with_headers(typed),
        )
        .await
        .expect("basic.publish")
        .await
        .expect("the server confirms the publish");
    let rejected = channel
        .basic_get(origin.as_str().into(), BasicGetOptions::default())
        .await
        .expect("basic.get")
        .expect("the message is on the queue");
    channel
        .basic_reject(rejected.delivery_tag, BasicRejectOptions { requeue: false })
        .await
        .expect("basic.reject");

    let connected = connect(&url).await;
    let mut subscriber = connected
        .subscribe(RabbitQueue::new(&dead).delay(Delay::dlx_ttl()))
        .await
        .expect("the waiting queue exists");
    let mut stream = Box::pin(subscriber.stream());
    let first = timeout_future(WAIT, stream.next())
        .await
        .expect("the dead letter arrives within timeout")
        .expect("stream has next")
        .expect("delivery ok");
    let death = first
        .headers()
        .get_str("x-death")
        .expect("the server's record");
    assert!(death.contains(r#""reason":"rejected""#), "{death}");
    first
        .nack_after(Duration::from_millis(100))
        .await
        .expect("the waiting queue takes the copy");

    let again = timeout_future(WAIT, stream.next())
        .await
        .expect("the copy comes back within timeout")
        .expect("stream has next")
        .expect("delivery ok");
    assert_eq!(again.payload(), b"later");
    assert_eq!(again.headers().get_str("x-attempt"), Some("3"));
    let death = again
        .headers()
        .get_str("x-death")
        .expect("the server's record");
    assert!(death.contains(r#""reason":"expired""#), "{death}");
    assert!(
        death.contains(&format!(r#""queue":"{waiting}""#)),
        "{death}"
    );
    again.ack().await.expect("ack");

    drop(stream);
    drop(subscriber);
    connected.shutdown().await.expect("shutdown");
    client.close(200, "OK".into()).await.expect("client close");
    delete_queues(&url, &[&origin, &dead, &waiting]).await;
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
