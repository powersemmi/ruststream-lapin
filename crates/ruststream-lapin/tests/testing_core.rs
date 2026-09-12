//! Integration tests for the in-process AMQP test broker.
//!
//! Most cases drive the public surface (`LapinTestBroker`, the stand-in publishers,
//! `LapinTestSubscriber`) directly, to keep failures localised; the `TestApp`-driven cases at
//! the end exercise the `TestableBroker` quiescence wiring (coordinator install,
//! `enqueued`/`consumed`) through the harness. Request/reply on the same transport lives in
//! `tests/request_reply_lapin.rs`, and the AMQP semantics the transport does not model
//! (bindings, dead-lettering, prefetch) in `tests/integration_lapin.rs` against a live
//! `RabbitMQ`.

#![cfg(feature = "testing")]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::{Stream, StreamExt};
use ruststream::runtime::{
    AppInfo, Ctx, HandlerOutcome, Reply, RustStream, State, SubscriberSettings,
};
use ruststream::subscriber;
use ruststream::testing::TestApp;
use ruststream::{
    BatchSubscriber, Broker, ConnectedBroker, DescribeServer, FromRef, HeaderMap, IncomingMessage,
    Outgoing, OutgoingMessage, Partitioned, Publisher, Subscriber, TransactionalPublisher, nonzero,
    testing::expect_published,
};
use ruststream_lapin::context::keys;
use ruststream_lapin::testing::{ConnectedLapinTestBroker, LapinTestBroker, LapinTestMessage};
use ruststream_lapin::{AmqpError, LapinPublish, PARTITION_KEY_HEADER, RabbitQueue};
use serde::{Deserialize, Serialize};

const WAIT: Duration = Duration::from_secs(1);

/// The in-process ladder, run for every test: synchronous construction then the consuming
/// `connect`, exactly like the real broker.
async fn connected() -> ConnectedLapinTestBroker {
    LapinTestBroker::new().connect().await.expect("connect")
}

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
    let broker = connected().await;

    let mut subscriber = broker.subscribe("orders").await.expect("subscribe");
    let publisher = broker.publisher(LapinPublish::default());

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
    let broker = connected().await;
    let publisher = broker.publisher(LapinPublish::default());
    let err = publisher
        .publish(OutgoingMessage::new("", b"x"))
        .await
        .expect_err("empty routing key must be rejected");
    assert!(format!("{err}").contains("routing key"), "got {err}");
}

// A queue with several consumers is a work queue: each delivery goes to exactly one of them, in
// rotation. A service that scales a handler horizontally must not see every copy processed twice
// in process when a server would hand each message out once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn competing_consumers_share_the_deliveries() {
    let broker = connected().await;
    let mut first = broker.subscribe("shared").await.expect("subscribe first");
    let mut second = broker.subscribe("shared").await.expect("subscribe second");
    let publisher = broker.publisher(LapinPublish::default());

    for payload in [b"m1".as_slice(), b"m2", b"m3", b"m4"] {
        publisher
            .publish(OutgoingMessage::new("shared", payload))
            .await
            .expect("publish");
    }

    let mut first_stream = Box::pin(first.stream());
    let mut second_stream = Box::pin(second.stream());
    assert_eq!(next_payload(&mut first_stream).await, b"m1");
    assert_eq!(next_payload(&mut second_stream).await, b"m2");
    assert_eq!(next_payload(&mut first_stream).await, b"m3");
    assert_eq!(next_payload(&mut second_stream).await, b"m4");
}

// A requeue goes back through the queue rather than to the consumer that rejected it, so the
// redelivery can land on a sibling - which is what a server does, and what makes a retry test
// with competing consumers mean anything.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_requeue_goes_back_to_the_queue() {
    let broker = connected().await;
    let mut first = broker.subscribe("retried").await.expect("subscribe first");
    let mut second = broker.subscribe("retried").await.expect("subscribe second");
    let publisher = broker.publisher(LapinPublish::default());

    publisher
        .publish(OutgoingMessage::new("retried", b"m1"))
        .await
        .expect("publish");

    let mut first_stream = Box::pin(first.stream());
    let msg = tokio::time::timeout(WAIT, first_stream.next())
        .await
        .expect("delivery within timeout")
        .expect("stream has next")
        .expect("delivery ok");
    assert!(!msg.redelivered(), "the first delivery is not a redelivery");
    msg.nack(true).await.expect("requeue");

    let mut second_stream = Box::pin(second.stream());
    let redelivered = tokio::time::timeout(WAIT, second_stream.next())
        .await
        .expect("redelivery within timeout")
        .expect("stream has next")
        .expect("delivery ok");
    assert_eq!(redelivered.payload(), b"m1");
    assert!(
        redelivered.redelivered(),
        "the copy that goes back is marked redelivered"
    );
    redelivered.ack().await.expect("ack");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn distinct_queues_are_isolated() {
    let broker = connected().await;
    let mut orders = broker.subscribe("orders").await.expect("subscribe orders");
    let mut events = broker.subscribe("events").await.expect("subscribe events");
    let publisher = broker.publisher(LapinPublish::default());

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
    let broker = connected().await;
    let mut subscriber = broker.subscribe("orders").await.expect("subscribe");
    let publisher = broker.publisher(LapinPublish::default());

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
    let broker = connected().await;
    let mut subscriber = broker.subscribe("orders").await.expect("subscribe");
    let publisher = broker.publisher(LapinPublish::default());

    let mut headers = HeaderMap::new();
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
    let broker = connected().await;
    let publisher = broker.publisher(LapinPublish::default());
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
    let broker = connected().await;
    let mut subscriber = broker.subscribe("orders").await.expect("subscribe");
    let publisher = broker.publisher(LapinPublish::default());

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
    let broker = connected().await;
    let mut sub = broker.subscribe("keyed").await.expect("subscribe");

    let mut headers = HeaderMap::new();
    headers.insert(PARTITION_KEY_HEADER, "tenant-a");
    broker
        .publisher(LapinPublish::default())
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
    let broker = connected().await;
    let mut sub = broker.subscribe("unkeyed").await.expect("subscribe");

    broker
        .publisher(LapinPublish::default())
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
    let broker = connected().await;
    let mut sub = broker.subscribe("tx").await.expect("subscribe");
    let publisher = broker.publisher(LapinPublish::default().confirms());

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
    let broker = connected().await;
    let publisher = broker.publisher(LapinPublish::default().confirms());

    publisher.begin_transaction().await.expect("begin");
    publisher
        .publish(OutgoingMessage::new("tx", b"discarded"))
        .await
        .expect("publish");
    publisher.abort().await.expect("abort");

    let observed = expect_published(&broker, "tx", 1, Duration::from_millis(50)).await;
    assert!(observed.is_empty(), "aborted messages must be discarded");
}

// The owned kind through the framework's typed sugar: `owned_transaction()` opens one transaction
// per call, each owning its buffer, so settling one never touches another.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn owned_transactions_settle_independently_through_the_typed_sugar() {
    use ruststream::runtime::PublishExt;

    let broker = connected().await;
    let publisher = broker.publisher(LapinPublish::default().confirms());

    let mut kept = publisher.owned_transaction().await.expect("open kept");
    let mut discarded = publisher.owned_transaction().await.expect("open discarded");
    kept.publish("orders", &Order { id: 1 })
        .await
        .expect("buffer kept");
    discarded
        .publish("orders", &Order { id: 2 })
        .await
        .expect("buffer discarded");

    discarded.abort().await.expect("abort");
    kept.commit().await.expect("commit");

    let observed = expect_published(&broker, "orders", 1, WAIT).await;
    assert_eq!(
        observed.len(),
        1,
        "only the committed transaction is routed"
    );
    assert_eq!(observed[0].payload(), br#"{"id":1}"#);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transaction_misuse_is_reported() {
    let broker = connected().await;
    let publisher = broker.publisher(LapinPublish::default().confirms());

    assert!(
        publisher.commit().await.is_err(),
        "commit with no open transaction must error"
    );
    assert!(
        publisher.abort().await.is_err(),
        "abort with no open transaction must error"
    );

    publisher.begin_transaction().await.expect("begin");
    assert!(
        publisher.begin_transaction().await.is_err(),
        "a second begin while one is open must error"
    );
    // The rejected begin must not have disturbed the open transaction.
    publisher
        .publish(OutgoingMessage::new("tx", b"kept"))
        .await
        .expect("publish inside the transaction");
    publisher.commit().await.expect("commit");

    let observed = expect_published(&broker, "tx", 1, WAIT).await;
    assert_eq!(observed.len(), 1, "the buffered message must be published");
}

// The ladder makes owner-side misuse a compile error; a publisher that outlives the shutdown is
// what stays checkable at runtime.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publishing_after_shutdown_errors() {
    let broker = connected().await;
    let publisher = broker.publisher(LapinPublish::default());
    broker.shutdown().await.expect("shutdown");

    let err = publisher
        .publish(OutgoingMessage::new("orders", b"late"))
        .await
        .expect_err("a publish through the closed transport must error");
    assert!(matches!(err, AmqpError::Closed { .. }), "got {err}");
}

// `Outgoing` without a name: `Order` is also the reply of the RPC handler below, whose
// destination is the requester's address rather than a property of the type.
#[derive(Serialize, Deserialize, PartialEq, Debug, Outgoing)]
struct Order {
    id: u64,
}

#[subscriber("orders")]
async fn ack_order(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::ack()
}

// The descriptor form must mount against the test broker through the testing-gated
// `SubscriptionSource<LapinTestBroker>` impl on `RabbitQueue`.
#[subscriber(RabbitQueue::new("payments"))]
async fn ack_payment(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::ack()
}

/// Counts how many times the retry handler ran, so the test can wire it as typed app state.
#[derive(Clone, Default)]
struct Attempts(Arc<AtomicUsize>);

#[subscriber(RabbitQueue::new("retry"))]
async fn retry_then_ack(order: &Order, ctx: &mut Context<'_, (), Attempts>) -> HandlerOutcome {
    let _ = order;
    // Requeue once, then acknowledge: exercises the `nack(requeue = true)` -> `enqueued`
    // re-count balanced against the delivery's `Drop` -> `consumed` decrement.
    if ctx.state().0.fetch_add(1, Ordering::SeqCst) == 0 {
        HandlerOutcome::retry()
    } else {
        HandlerOutcome::ack()
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
        .settled(HandlerOutcome::ack());
    tb.broker::<LapinTestBroker>()
        .subscriber("payments")
        .assert_called_once()
        .with(&Order { id: 2 })
        .settled(HandlerOutcome::ack());

    tb.shutdown().await.expect("shutdown");
}

/// One delivery as the `Ctx` handler below saw it.
#[derive(Clone, Debug, PartialEq, Eq)]
struct SeenDelivery {
    exchange: String,
    routing_key: String,
    redelivered: bool,
    delivery_tag: u64,
}

/// What that handler saw, one entry per delivery.
#[derive(Clone, Default)]
struct Seen(Arc<Mutex<Vec<SeenDelivery>>>);

#[derive(Clone, FromRef)]
struct MetadataState {
    seen: Seen,
}

#[subscriber(RabbitQueue::new("metadata"))]
async fn record_metadata(
    order: &Order,
    Ctx(exchange): Ctx<keys::Exchange>,
    Ctx(routing_key): Ctx<keys::RoutingKey>,
    Ctx(redelivered): Ctx<keys::Redelivered>,
    Ctx(delivery_tag): Ctx<keys::DeliveryTag>,
    State(seen): State<Seen>,
) -> HandlerOutcome {
    let _ = order;
    seen.0
        .lock()
        .expect("seen mutex poisoned")
        .push(SeenDelivery {
            exchange,
            routing_key,
            redelivered,
            delivery_tag,
        });
    HandlerOutcome::ack()
}

// A handler that binds AMQP delivery fields must mount on the in-process broker too: the
// transport reports them against its own model (default exchange, queue name as the routing key,
// per-subscription delivery tags), so the same handler is unit-testable without a server.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ctx_keys_resolve_against_the_in_process_transport() {
    let seen = Seen::default();
    let probe = seen.clone();
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .on_startup(
            move |()| async move { Ok::<_, std::convert::Infallible>(MetadataState { seen }) },
        )
        .with_broker(LapinTestBroker::new(), |b| {
            b.include(record_metadata);
        });
    let tb = TestApp::start(app).await.expect("start");

    tb.broker::<LapinTestBroker>()
        .publish("metadata", &Order { id: 3 })
        .await
        .expect("publish must drive the reaction to quiescence");

    let seen = probe.0.lock().expect("seen mutex poisoned").clone();
    assert_eq!(
        seen,
        vec![SeenDelivery {
            exchange: String::new(),
            routing_key: "metadata".to_owned(),
            redelivered: false,
            delivery_tag: 1,
        }],
        "the default exchange, the queue name as the routing key, a first delivery, tag 1"
    );

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
        .settled(HandlerOutcome::ack());

    tb.shutdown().await.expect("shutdown");
}

#[subscriber(RabbitQueue::new("rpc.in"), publish("rpc.fallback"))]
async fn echo_id(order: &Order) -> Result<Order, HandlerOutcome> {
    Ok(Order { id: order.id })
}

// The exported DirectReplyTo transform must redirect each reply to the request's reply-to,
// echo its correlation id, and fall through to the static destination without one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn direct_reply_transform_redirects_and_echoes() {
    use ruststream::testing::TestableBroker;
    use ruststream_lapin::DirectReplyTo;

    let broker = LapinTestBroker::new();
    // A second handle on the same in-process transport: the app owns one end of the ladder, the
    // test injects and observes through the other.
    let probe = broker.clone().connect().await.expect("connect");
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(broker, |b| {
        b.include(echo_id)
            .out(Reply, LapinPublish::default())
            .transform(DirectReplyTo);
    });

    // TestApp drives the lifecycle (subscriptions are open once `start` returns); the requests
    // are injected raw on the shared broker because they must carry headers, which the harness
    // publish API does not accept.
    let tb = TestApp::start(app).await.expect("start");

    let mut headers = HeaderMap::new();
    headers.insert("reply-to", "rpc.replies");
    headers.insert("correlation-id", "c-9");
    probe.inject(OutgoingMessage::new("rpc.in", br#"{"id":9}"#).with_headers(headers));

    let redirected = expect_published(&probe, "rpc.replies", 1, Duration::from_secs(1)).await;
    assert_eq!(
        redirected.len(),
        1,
        "reply must land on the request's reply-to address"
    );
    assert_eq!(redirected[0].headers().correlation_id(), Some("c-9"));

    probe.inject(OutgoingMessage::new("rpc.in", br#"{"id":1}"#));
    let fallback = expect_published(&probe, "rpc.fallback", 1, Duration::from_secs(1)).await;
    assert_eq!(
        fallback.len(),
        1,
        "a request without reply-to falls through to the mount name"
    );

    tb.shutdown().await.expect("shutdown");
}

/// The confirmation of one order, addressed by its own declaration.
#[derive(Serialize, Deserialize, PartialEq, Debug, Outgoing)]
#[outgoing(name = "confirmations")]
struct Confirmation {
    id: u64,
}

#[subscriber("orders.declared", publish)]
async fn confirm_order(order: &Order) -> Confirmation {
    Confirmation { id: order.id }
}

/// A receipt with no declared destination: the mount site says where it goes.
#[derive(Serialize, Deserialize, PartialEq, Debug, Outgoing)]
struct Receipt {
    id: u64,
}

#[subscriber("orders.mounted", publish("receipts"))]
async fn receipt_for(order: &Order) -> Receipt {
    Receipt { id: order.id }
}

// A reply type that declares its own destination publishes there, on the queue name the AMQP
// publisher turns into a routing key.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_declared_reply_lands_where_its_type_says() {
    let app =
        RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(LapinTestBroker::new(), |b| {
            b.include(confirm_order).out(Reply, LapinPublish::default());
        });
    let tb = TestApp::start(app).await.expect("start");

    tb.broker::<LapinTestBroker>()
        .publish("orders.declared", &Order { id: 4 })
        .await
        .expect("publish must drive the reply to quiescence");

    tb.broker::<LapinTestBroker>()
        .subscriber("orders.declared")
        .assert_called_once();
    tb.broker::<LapinTestBroker>()
        .published::<Confirmation>("confirmations")
        .assert_called_once()
        .with(&Confirmation { id: 4 });

    tb.shutdown().await.expect("shutdown");
}

// A reply type that declares nothing takes the mount site's name, which is what the direct
// reply-to fallback rests on.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_undeclared_reply_lands_at_the_mount_name() {
    let app =
        RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(LapinTestBroker::new(), |b| {
            b.include(receipt_for).out(Reply, LapinPublish::default());
        });
    let tb = TestApp::start(app).await.expect("start");

    tb.broker::<LapinTestBroker>()
        .publish("orders.mounted", &Order { id: 5 })
        .await
        .expect("publish must drive the reply to quiescence");

    tb.broker::<LapinTestBroker>()
        .subscriber("orders.mounted")
        .assert_called_once();
    tb.broker::<LapinTestBroker>()
        .published::<Receipt>("receipts")
        .assert_called_once()
        .with(&Receipt { id: 5 });

    tb.shutdown().await.expect("shutdown");
}

// AMQP has no wire batch, so the in-process transport batches the way the real subscriber does -
// on the client, capped by the size the stream was opened with. Everything is already queued
// when the stream is first polled, so the batches close on the size rather than on a deadline.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batches_are_capped_by_the_size_the_stream_is_opened_with() {
    let broker = connected().await;
    let mut subscriber = broker.subscribe("batches").await.expect("subscribe");
    let publisher = broker.publisher(LapinPublish::default());
    for payload in [b"p1".as_slice(), b"p2", b"p3", b"p4", b"p5"] {
        publisher
            .publish(OutgoingMessage::new("batches", payload))
            .await
            .expect("publish");
    }

    let mut sizes = Vec::new();
    let mut payloads: Vec<Vec<u8>> = Vec::new();
    let mut stream = Box::pin(subscriber.batches(nonzero!(2)));
    while payloads.len() < 5 {
        let batch = tokio::time::timeout(WAIT, stream.next())
            .await
            .expect("batch within timeout")
            .expect("stream has next")
            .expect("batch ok");
        sizes.push(batch.len());
        for msg in batch {
            payloads.push(msg.payload().to_vec());
            msg.ack().await.expect("ack");
        }
    }

    assert_eq!(
        sizes,
        vec![2, 2, 1],
        "a batch never carries more than its size"
    );
    assert_eq!(
        payloads,
        vec![
            b"p1".to_vec(),
            b"p2".to_vec(),
            b"p3".to_vec(),
            b"p4".to_vec(),
            b"p5".to_vec()
        ],
        "batching preserves the publish order across batches"
    );
}

#[subscriber(RabbitQueue::new("batches.settled"))]
async fn settle_batch(orders: &[Order]) -> HandlerOutcome {
    let _ = orders.len();
    HandlerOutcome::ack()
}

// A batch handler mounts on the in-process transport exactly as it does on a server: the
// capability is there either way, and the harness reports the batches the body was handed. Each
// publish returns at quiescence, so each delivery arrives as a batch of its own; that the size
// caps a fuller batch is proven against the transport above and against a server by the
// conformance batch suite.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_handlers_mount_on_the_in_process_transport() {
    let app =
        RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(LapinTestBroker::new(), |b| {
            b.include(settle_batch.batch(nonzero!(4)));
        });
    let tb = TestApp::start(app).await.expect("start");

    for id in 1..=2 {
        tb.broker::<LapinTestBroker>()
            .publish("batches.settled", &Order { id })
            .await
            .expect("publish must drive the batch to quiescence");
    }

    tb.broker::<LapinTestBroker>()
        .subscriber("batches.settled")
        .assert_batch_sizes(&[1, 1])
        .settled(HandlerOutcome::ack());

    tb.shutdown().await.expect("shutdown");
}
