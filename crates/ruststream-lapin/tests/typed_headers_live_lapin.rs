//! Typed header-table values from outside the service, read by a service on a real `RabbitMQ`.
//!
//! A Java or Python client writes numbers, flags and nested tables with their own AMQP types, and
//! the server stamps its own `x-death` record on a dead-lettered message. A service reads every
//! one of them as a header. Every test is a no-op unless `AMQP_TEST_URL` points at a broker (see
//! `just test-brokers`).
//!
//! The foreign message is confirmed onto the queue before the harness publishes a message of its
//! own there. A live harness waits until the subscription has handled as many deliveries as it saw
//! published to it, and the one consumer takes the queue in order, so the delivery it has handled
//! by then is the foreign one.

#![cfg(feature = "testing")]

use std::convert::Infallible;
use std::sync::{Arc, Mutex};

use lapin::options::{
    BasicGetOptions, BasicPublishOptions, BasicRejectOptions, ConfirmSelectOptions,
    QueueDeclareOptions, QueueDeleteOptions,
};
use lapin::types::{DecimalValue, FieldArray, LongString, ShortString};
use lapin::{BasicProperties, Channel, Connection, ConnectionProperties};
use ruststream::Outgoing;
use ruststream::testing::TestApp;
use ruststream_lapin::prelude::*;
use serde::{Deserialize, Serialize};

use crate::live;

fn amqp_url() -> Option<String> {
    live::url("AMQP_TEST_URL")
}

/// The queue a foreign producer publishes typed headers to.
const FOREIGN: &str = "ruststream-typed-headers.foreign";
/// The queue a raw client rejects a message on, and the queue the server dead-letters it to.
const REJECTED: &str = "ruststream-typed-headers.rejected";
const REJECTED_DEAD: &str = "ruststream-typed-headers.rejected.dead";

#[derive(Debug, Outgoing, Serialize, Deserialize)]
struct Order {
    id: u64,
}

/// The headers of every delivery a handler saw, in delivery order.
#[derive(Clone, Default)]
struct Seen(Arc<Mutex<Vec<HeaderMap>>>);

impl Seen {
    fn headers(&self) -> Vec<HeaderMap> {
        self.0.lock().expect("seen mutex poisoned").clone()
    }
}

fn record(order: &Order, ctx: &Context<'_, (), Seen>) -> HandlerOutcome {
    let _ = order.id;
    let headers = ctx.headers().clone();
    ctx.state()
        .0
        .lock()
        .expect("seen mutex poisoned")
        .push(headers);
    HandlerOutcome::ack()
}

#[subscriber(RabbitQueue::new(FOREIGN))]
async fn foreign(order: &Order, ctx: &mut Context<'_, (), Seen>) -> HandlerOutcome {
    record(order, ctx)
}

#[subscriber(RabbitQueue::new(REJECTED_DEAD))]
async fn dead(order: &Order, ctx: &mut Context<'_, (), Seen>) -> HandlerOutcome {
    record(order, ctx)
}

// A foreign producer's typed headers reach the handler as their documented text, and its string
// header byte for byte.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_headers_from_a_foreign_producer_reach_the_handler() {
    let Some(url) = amqp_url() else { return };
    delete_queues(&url, &[FOREIGN]).await;

    let seen = Seen::default();
    let state = seen.clone();
    let app = RustStream::new(AppInfo::new("foreign", "0.1.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(state))
        .with_broker(LapinBroker::new(url.clone()).declare_topology(true), |b| {
            b.include(foreign);
        });
    let tb = TestApp::start_live(app).await.expect("start");

    let mut table = FieldTable::default();
    for (name, value) in [
        ("x-attempt", AMQPValue::LongInt(3)),
        ("x-urgent", AMQPValue::Boolean(true)),
        ("x-sent-at", AMQPValue::Timestamp(1_700_000_000)),
        (
            "x-price",
            AMQPValue::DecimalValue(DecimalValue {
                scale: 2,
                value: 1999,
            }),
        ),
        ("x-ratio", AMQPValue::Double(0.25)),
        (
            "x-tags",
            AMQPValue::FieldArray(FieldArray::from(vec![
                AMQPValue::LongString("red".into()),
                AMQPValue::LongLongInt(7),
            ])),
        ),
        ("x-tenant", AMQPValue::LongString("acme".into())),
    ] {
        table.insert(ShortString::from(name), value);
    }
    let producer = Connection::connect(&url, ConnectionProperties::default())
        .await
        .expect("producer connect");
    let channel = producer.create_channel().await.expect("producer channel");
    publish_confirmed(
        &channel,
        FOREIGN,
        br#"{"id":1}"#,
        BasicProperties::default().with_headers(table),
    )
    .await;
    producer
        .close(200, "OK".into())
        .await
        .expect("producer close");

    tb.broker::<LapinBroker>()
        .message(&Order { id: 2 })
        .to(FOREIGN)
        .publish()
        .await
        .expect("the subscription handles a delivery");

    let deliveries = seen.headers();
    let headers = deliveries
        .first()
        .expect("the foreign delivery was handled");
    for (name, text) in [
        ("x-attempt", "3"),
        ("x-urgent", "true"),
        ("x-sent-at", "1700000000"),
        ("x-price", "19.99"),
        ("x-ratio", "0.25"),
        ("x-tags", r#"["red",7]"#),
        ("x-tenant", "acme"),
    ] {
        assert_eq!(headers.get_str(name), Some(text), "header {name}");
    }

    tb.shutdown().await.expect("shutdown");
    delete_queues(&url, &[FOREIGN]).await;
}

// A message the server dead-lettered carries the server's own record of it, and the handler on
// the dead-letter queue reads that record as JSON.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_servers_dead_letter_record_reaches_the_handler() {
    let Some(url) = amqp_url() else { return };
    delete_queues(&url, &[REJECTED, REJECTED_DEAD]).await;

    // A raw client rejects the message, so the server moves it before the service starts.
    let client = Connection::connect(&url, ConnectionProperties::default())
        .await
        .expect("client connect");
    let channel = client.create_channel().await.expect("client channel");
    declare(&channel, REJECTED_DEAD, FieldTable::default()).await;
    declare(&channel, REJECTED, arguments_to_dead()).await;
    publish_confirmed(
        &channel,
        REJECTED,
        br#"{"id":1}"#,
        BasicProperties::default(),
    )
    .await;
    let message = channel
        .basic_get(REJECTED.into(), BasicGetOptions::default())
        .await
        .expect("basic.get")
        .expect("the message is on the queue");
    channel
        .basic_reject(message.delivery_tag, BasicRejectOptions { requeue: false })
        .await
        .expect("basic.reject");
    // The queue handles this channel's frames in order, so by the time it answers the declare it
    // has handled the rejection and handed the copy to the dead-letter queue.
    assert_eq!(declare(&channel, REJECTED, arguments_to_dead()).await, 0);
    assert_eq!(
        declare(&channel, REJECTED_DEAD, FieldTable::default()).await,
        1,
        "the server dead-lettered the message",
    );
    client.close(200, "OK".into()).await.expect("client close");

    let seen = Seen::default();
    let state = seen.clone();
    let app = RustStream::new(AppInfo::new("dead", "0.1.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(state))
        .with_broker(LapinBroker::new(url.clone()), |b| {
            b.include(dead);
        });
    let tb = TestApp::start_live(app).await.expect("start");
    tb.broker::<LapinBroker>()
        .message(&Order { id: 2 })
        .to(REJECTED_DEAD)
        .publish()
        .await
        .expect("the subscription handles a delivery");

    let deliveries = seen.headers();
    let headers = deliveries
        .first()
        .expect("the foreign delivery was handled");
    let death = headers.get_str("x-death").expect("the x-death record");
    assert!(
        death.starts_with(r#"[{"count":1,"#),
        "one death, as JSON: {death}"
    );
    for field in [
        r#""exchange":"""#,
        &format!(r#""queue":"{REJECTED}""#),
        r#""reason":"rejected""#,
        &format!(r#""routing-keys":["{REJECTED}"]"#),
        r#""time":"#,
    ] {
        assert!(death.contains(field), "{field} in {death}");
    }
    assert_eq!(headers.get_str("x-first-death-queue"), Some(REJECTED));
    assert_eq!(headers.get_str("x-first-death-reason"), Some("rejected"));

    tb.shutdown().await.expect("shutdown");
    delete_queues(&url, &[REJECTED, REJECTED_DEAD]).await;
}

/// The arguments of a queue that dead-letters what it rejects to [`REJECTED_DEAD`].
fn arguments_to_dead() -> FieldTable {
    let mut arguments = FieldTable::default();
    arguments.insert(
        ShortString::from("x-dead-letter-exchange"),
        AMQPValue::LongString(LongString::from("")),
    );
    arguments.insert(
        ShortString::from("x-dead-letter-routing-key"),
        AMQPValue::LongString(LongString::from(REJECTED_DEAD)),
    );
    arguments
}

async fn publish_confirmed(
    channel: &Channel,
    queue: &str,
    payload: &[u8],
    properties: BasicProperties,
) {
    channel
        .confirm_select(ConfirmSelectOptions::default())
        .await
        .expect("confirm.select");
    channel
        .basic_publish(
            ShortString::default(),
            queue.into(),
            BasicPublishOptions::default(),
            payload,
            properties.with_content_type("application/json".into()),
        )
        .await
        .expect("basic.publish")
        .await
        .expect("the server confirms the publish");
}

/// Declares the durable queue `queue` with `arguments`, and returns how many messages it holds.
async fn declare(channel: &Channel, queue: &str, arguments: FieldTable) -> u32 {
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
        .expect("queue.declare")
        .message_count()
}

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
