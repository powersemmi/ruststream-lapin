//! Integration tests for the in-process AMQP test broker.
//!
//! Most cases drive the public surface (`LapinTestBroker`, `LapinTestPublisher`,
//! `LapinTestSubscriber`) directly, to keep failures localised; the `TestApp`-driven cases at
//! the end exercise the `TestableBroker` quiescence wiring (coordinator install,
//! `enqueued`/`consumed`) through the harness. Real AMQP semantics (bindings, dead-lettering,
//! prefetch, request/reply) live in `tests/integration_lapin.rs` against a live `RabbitMQ`.

#![cfg(feature = "testing")]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures::{Stream, StreamExt};
use ruststream::runtime::{AppInfo, HandlerResult, RustStream};
use ruststream::subscriber;
use ruststream::testing::TestApp;
use ruststream::{
    Broker, DescribeServer, Headers, IncomingMessage, OutgoingMessage, Partitioned, Publisher,
    Subscriber, TransactionalPublisher, testing::expect_published,
};
use ruststream_lapin::testing::{LapinTestBroker, LapinTestMessage};
use ruststream_lapin::{AmqpError, PARTITION_KEY_HEADER, RabbitQueue};
use serde::{Deserialize, Serialize};

const WAIT: Duration = Duration::from_secs(1);

async fn next_payload<S>(stream: &mut S) -> Vec<u8>
where
    S: Stream<Item = Result<LapinTestMessage, AmqpError>> + Unpin,
{
    let msg = tokio::time::timeout(WAIT, stream.next())
        .await
        .expect("delivery within timeout")
        .expect("stream has next")
        .expect("delivery ok");
    let payload = msg.payload().to_vec();
    msg.ack().await.expect("ack");
    payload
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pub_sub_round_trip_through_broker_traits() {
    let broker = LapinTestBroker::new();
    broker.connect().await.expect("connect");

    let mut subscriber = broker.subscribe("orders").await.expect("subscribe");
    let publisher = broker.publisher();

    publisher
        .publish(OutgoingMessage::new("orders", b"o1"))
        .await
        .expect("publish");

    let mut stream = Box::pin(subscriber.stream());
    let got = next_payload(&mut stream).await;
    assert_eq!(got, b"o1");
    drop(stream);

    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publisher_rejects_empty_routing_key() {
    let broker = LapinTestBroker::new();
    let publisher = broker.publisher();
    let err = publisher
        .publish(OutgoingMessage::new("", b"x"))
        .await
        .expect_err("empty routing key must be rejected");
    assert!(format!("{err}").contains("routing key"), "got {err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn distinct_queues_are_isolated() {
    let broker = LapinTestBroker::new();
    let mut orders = broker.subscribe("orders").await.expect("subscribe orders");
    let mut events = broker.subscribe("events").await.expect("subscribe events");
    let publisher = broker.publisher();

    publisher
        .publish(OutgoingMessage::new("orders", b"o"))
        .await
        .expect("publish o");
    publisher
        .publish(OutgoingMessage::new("events", b"e"))
        .await
        .expect("publish e");

    let mut orders_stream = Box::pin(orders.stream());
    assert_eq!(next_payload(&mut orders_stream).await, b"o");

    let mut events_stream = Box::pin(events.stream());
    assert_eq!(next_payload(&mut events_stream).await, b"e");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nack_requeue_redelivers_to_same_subscriber() {
    let broker = LapinTestBroker::new();
    let mut subscriber = broker.subscribe("orders").await.expect("subscribe");
    let publisher = broker.publisher();

    publisher
        .publish(OutgoingMessage::new("orders", b"once"))
        .await
        .expect("publish");

    let mut stream = Box::pin(subscriber.stream());
    let first = tokio::time::timeout(WAIT, stream.next())
        .await
        .expect("first delivery")
        .expect("stream has next")
        .expect("ok");
    first.nack(true).await.expect("nack requeue");

    let second = tokio::time::timeout(WAIT, stream.next())
        .await
        .expect("redelivery")
        .expect("stream has next")
        .expect("ok");
    assert_eq!(second.payload(), b"once");
    second.ack().await.expect("ack");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn headers_are_propagated_to_subscribers() {
    let broker = LapinTestBroker::new();
    let mut subscriber = broker.subscribe("orders").await.expect("subscribe");
    let publisher = broker.publisher();

    let mut headers = Headers::new();
    headers.insert("content-type", "application/json");
    headers.insert("correlation-id", "abc-1");
    let outgoing = OutgoingMessage::new("orders", b"{}").with_headers(headers);
    publisher.publish(outgoing).await.expect("publish");

    let mut stream = Box::pin(subscriber.stream());
    let msg = tokio::time::timeout(WAIT, stream.next())
        .await
        .expect("delivery")
        .expect("stream has next")
        .expect("ok");
    assert_eq!(msg.headers().content_type(), Some("application/json"));
    assert_eq!(msg.headers().correlation_id(), Some("abc-1"));
    msg.ack().await.expect("ack");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn expect_published_observes_publishes() {
    let broker = LapinTestBroker::new();
    let publisher = broker.publisher();
    publisher
        .publish(OutgoingMessage::new("events", b"first"))
        .await
        .expect("publish first");
    publisher
        .publish(OutgoingMessage::new("events", b"second"))
        .await
        .expect("publish second");
    let observed = expect_published(&broker, "events", 2, Duration::from_secs(1)).await;
    assert_eq!(observed.len(), 2);
    assert_eq!(observed[0].payload(), b"first");
    assert_eq!(observed[1].payload(), b"second");
    broker.shutdown().await.expect("shutdown");
}

// The Subscriber contract (and the conformance helpers) re-enter `stream()` per call.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_can_be_reentered() {
    let broker = LapinTestBroker::new();
    let mut subscriber = broker.subscribe("orders").await.expect("subscribe");
    let publisher = broker.publisher();

    publisher
        .publish(OutgoingMessage::new("orders", b"one"))
        .await
        .expect("publish one");
    {
        let mut stream = Box::pin(subscriber.stream());
        assert_eq!(next_payload(&mut stream).await, b"one");
    }

    publisher
        .publish(OutgoingMessage::new("orders", b"two"))
        .await
        .expect("publish two");
    let mut stream = Box::pin(subscriber.stream());
    assert_eq!(next_payload(&mut stream).await, b"two");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partition_key_header_is_surfaced() {
    let broker = LapinTestBroker::new();
    let mut sub = broker.subscribe("keyed").await.expect("subscribe");

    let mut headers = Headers::new();
    headers.insert(PARTITION_KEY_HEADER, "tenant-a");
    broker
        .publisher()
        .publish(OutgoingMessage::new("keyed", b"payload").with_headers(headers))
        .await
        .expect("publish");

    let mut stream = Box::pin(sub.stream());
    let msg = tokio::time::timeout(WAIT, stream.next())
        .await
        .expect("delivery")
        .expect("item")
        .expect("ok");
    assert_eq!(
        Partitioned::partition_key(&msg),
        Some(b"tenant-a".as_slice())
    );
    // The IncomingMessage override sees the same key (the path keyed lanes use).
    assert_eq!(
        IncomingMessage::partition_key(&msg),
        Some(b"tenant-a".as_slice())
    );
    msg.ack().await.ok();
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partition_key_absent_yields_none() {
    let broker = LapinTestBroker::new();
    let mut sub = broker.subscribe("unkeyed").await.expect("subscribe");

    broker
        .publisher()
        .publish(OutgoingMessage::new("unkeyed", b"payload"))
        .await
        .expect("publish");

    let mut stream = Box::pin(sub.stream());
    let msg = tokio::time::timeout(WAIT, stream.next())
        .await
        .expect("delivery")
        .expect("item")
        .expect("ok");
    assert_eq!(Partitioned::partition_key(&msg), None);
    msg.ack().await.ok();
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn describe_server_returns_amqp_protocol() {
    let broker = LapinTestBroker::new();
    let spec = broker.describe_server();
    assert_eq!(spec.protocol, "amqp");
    assert_eq!(spec.host, None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transaction_buffers_until_commit() {
    let broker = LapinTestBroker::new();
    let mut sub = broker.subscribe("tx").await.expect("subscribe");
    let publisher = broker.publisher();

    publisher.begin_transaction().await.expect("begin");
    publisher
        .publish(OutgoingMessage::new("tx", b"first"))
        .await
        .expect("publish first");
    publisher
        .publish(OutgoingMessage::new("tx", b"second"))
        .await
        .expect("publish second");

    // Nothing is visible before commit.
    let observed = expect_published(&broker, "tx", 1, Duration::from_millis(50)).await;
    assert!(observed.is_empty(), "buffered messages must not be visible");

    publisher.commit().await.expect("commit");

    let mut stream = Box::pin(sub.stream());
    assert_eq!(next_payload(&mut stream).await, b"first");
    assert_eq!(next_payload(&mut stream).await, b"second");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transaction_abort_discards_buffer() {
    let broker = LapinTestBroker::new();
    let publisher = broker.publisher();

    publisher.begin_transaction().await.expect("begin");
    publisher
        .publish(OutgoingMessage::new("tx", b"discarded"))
        .await
        .expect("publish");
    publisher.abort().await.expect("abort");

    let observed = expect_published(&broker, "tx", 1, Duration::from_millis(50)).await;
    assert!(observed.is_empty(), "aborted messages must be discarded");
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct Order {
    id: u64,
}

#[subscriber("orders")]
async fn ack_order(order: &Order) -> HandlerResult {
    let _ = order;
    HandlerResult::Ack
}

// The descriptor form must mount against the test broker through the testing-gated
// `SubscriptionSource<LapinTestBroker>` impl on `RabbitQueue`.
#[subscriber(RabbitQueue::new("payments"))]
async fn ack_payment(order: &Order) -> HandlerResult {
    let _ = order;
    HandlerResult::Ack
}

/// Counts how many times the retry handler ran, so the test can wire it as typed app state.
#[derive(Clone, Default)]
struct Attempts(Arc<AtomicUsize>);

#[subscriber(RabbitQueue::new("retry"))]
async fn retry_then_ack(order: &Order, ctx: &mut Context<'_, (), Attempts>) -> HandlerResult {
    let _ = order;
    // Requeue once, then acknowledge: exercises the `nack(requeue = true)` -> `enqueued`
    // re-count balanced against the delivery's `Drop` -> `consumed` decrement.
    if ctx.state().0.fetch_add(1, Ordering::SeqCst) == 0 {
        HandlerResult::retry()
    } else {
        HandlerResult::Ack
    }
}

// The harness installs its coordinator into `LapinTestBroker`, so `publish` must drive the
// in-process reaction to quiescence (every `enqueued` balanced by a `consumed`) before
// returning.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_app_drives_lapin_test_broker_to_quiescence() {
    let app =
        RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(LapinTestBroker::new(), |b| {
            b.include(ack_order);
            b.include(ack_payment);
        });
    let tb = TestApp::start(app).await.expect("start");

    tb.broker::<LapinTestBroker>()
        .publish("orders", &Order { id: 1 })
        .await
        .expect("publish must drive the reaction to quiescence");
    tb.broker::<LapinTestBroker>()
        .publish("payments", &Order { id: 2 })
        .await
        .expect("publish must drive the descriptor-mounted reaction to quiescence");

    tb.broker::<LapinTestBroker>()
        .subscriber("orders")
        .assert_called_once()
        .with(&Order { id: 1 })
        .settled(HandlerResult::Ack);
    tb.broker::<LapinTestBroker>()
        .subscriber("payments")
        .assert_called_once()
        .with(&Order { id: 2 })
        .settled(HandlerResult::Ack);

    tb.shutdown().await.expect("shutdown");
}

// A requeue re-enqueues a fresh delivery, so the harness must still reach quiescence: the
// second delivery's ack balances the count. The handler is called exactly twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_app_requeue_stays_balanced() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .on_startup(|()| async { Ok::<_, std::convert::Infallible>(Attempts::default()) })
        .with_broker(LapinTestBroker::new(), |b| {
            b.include(retry_then_ack);
        });
    let tb = TestApp::start(app).await.expect("start");

    tb.broker::<LapinTestBroker>()
        .publish("retry", &Order { id: 7 })
        .await
        .expect("publish must drive the requeue reaction to quiescence");

    tb.broker::<LapinTestBroker>()
        .subscriber("retry")
        .assert_called(2)
        .settled(HandlerResult::Ack);

    tb.shutdown().await.expect("shutdown");
}

#[subscriber(RabbitQueue::new("rpc.in"), publish("rpc.fallback"))]
async fn echo_id(order: &Order) -> Result<Order, HandlerResult> {
    Ok(Order { id: order.id })
}

// The exported DirectReplyTo transform must redirect each reply to the request's reply-to,
// echo its correlation id, and fall through to the static destination without one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn direct_reply_transform_redirects_and_echoes() {
    use ruststream::runtime::TypedPublisher;
    use ruststream::testing::TestableBroker;
    use ruststream_lapin::DirectReplyTo;

    let broker = LapinTestBroker::new();
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(broker.clone(), |b| {
        let replies = TypedPublisher::new(b.broker().publisher()).transform(DirectReplyTo);
        b.include_publishing(echo_id, replies);
    });

    // TestApp drives the lifecycle (subscriptions are open once `start` returns); the requests
    // are injected raw on the shared broker because they must carry headers, which the harness
    // publish API does not accept.
    let tb = TestApp::start(app).await.expect("start");

    let mut headers = Headers::new();
    headers.insert("reply-to", "rpc.replies");
    headers.insert("correlation-id", "c-9");
    broker.inject(OutgoingMessage::new("rpc.in", br#"{"id":9}"#).with_headers(headers));

    let redirected = expect_published(&broker, "rpc.replies", 1, Duration::from_secs(1)).await;
    assert_eq!(
        redirected.len(),
        1,
        "reply must land on the request's reply-to address"
    );
    assert_eq!(redirected[0].headers().correlation_id(), Some("c-9"));

    broker.inject(OutgoingMessage::new("rpc.in", br#"{"id":1}"#));
    let fallback = expect_published(&broker, "rpc.fallback", 1, Duration::from_secs(1)).await;
    assert_eq!(
        fallback.len(),
        1,
        "a request without reply-to falls through to the mount name"
    );

    tb.shutdown().await.expect("shutdown");
}
