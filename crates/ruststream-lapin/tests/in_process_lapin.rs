//! The broker's in-process mode: what `connect_in_process` connects, driven directly where the
//! transport is the subject, and through the `TestApp` harness where the production app is.
//!
//! The direct cases open subscriptions and publishers on the connected form the harness would
//! connect, to keep failures localised; the harness cases run a service's app unchanged and
//! address the broker by its production type. Request/reply on the same transport lives in
//! `tests/request_reply_lapin.rs`; what only a server does is covered against a live `RabbitMQ`
//! by `tests/integration_lapin.rs`.

#![cfg(feature = "testing")]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::{Stream, StreamExt};
use ruststream::runtime::{
    AppInfo, Ctx, ForReply, HandlerOutcome, Outgoing, PublishContext, PublishTransform,
    RETRY_COUNT_HEADER, Reads, Reply, RustStream, State, SubscriberSettings,
};
use ruststream::subscriber;
use ruststream::testing::{InProcess, TestApp, expect_published};
use ruststream::{
    AckError, BatchSubscriber, ConnectedBroker, FromRef, HeaderMap, IncomingMessage, Outgoing,
    OutgoingMessage, Partitioned, Publisher, Subscriber, TransactionalPublisher, nonzero,
};
use ruststream_lapin::context::keys;
use ruststream_lapin::{
    AMQPValue, AmqpError, ConnectedLapinBroker, FieldTable, LapinBroker, LapinMessage,
    LapinPublish, LapinSubscriber, PARTITION_KEY_HEADER, RabbitExchange, RabbitQueue,
    RabbitQuorumQueue,
};
use serde::{Deserialize, Serialize};

const WAIT: Duration = Duration::from_secs(1);

/// The address the service's broker is built with. The in-process mode dials nothing, so no
/// server has to answer here.
const URI: &str = "amqp://localhost:5672";

/// The transition the harness connects through, run for every direct case: the production
/// broker, connected in process.
async fn connected() -> ConnectedLapinBroker {
    LapinBroker::new(URI)
        .connect_in_process()
        .await
        .expect("connect in process")
}

/// The same, on a broker that declares the topology its descriptors describe.
async fn declaring() -> ConnectedLapinBroker {
    LapinBroker::new(URI)
        .declare_topology(true)
        .connect_in_process()
        .await
        .expect("connect in process")
}

async fn next_payload<S>(stream: &mut S) -> Vec<u8>
where
    S: Stream<Item = Result<LapinMessage, AmqpError>> + Unpin,
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

    let mut subscriber = broker
        .subscribe(RabbitQueue::new("orders"))
        .await
        .expect("subscribe");
    let publisher = broker.publisher(LapinPublish::default());

    publisher
        .publish(OutgoingMessage::new("orders", b"o1"), None)
        .await
        .expect("publish");

    let mut stream = Box::pin(subscriber.stream());
    let got = next_payload(&mut stream).await;
    assert_eq!(got, b"o1");
    drop(stream);

    broker.shutdown().await.expect("shutdown");
}

// A routing key is an AMQP short string, and the server refuses a longer one; the in-process
// publish checks it with the same conversion.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_routing_key_the_protocol_cannot_carry_is_refused() {
    let broker = connected().await;
    let publisher = broker.publisher(LapinPublish::default());
    let too_long = "k".repeat(256);

    let err = publisher
        .publish(OutgoingMessage::new(&too_long, b"x"), None)
        .await
        .expect_err("a 256-byte routing key is not a short string");

    assert!(matches!(err, AmqpError::InvalidOptions(_)), "got {err}");
}

// An empty routing key on the default exchange is a valid publish that reaches no queue: the
// server takes it and routes it nowhere, and so does the in-process transport.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_empty_routing_key_is_unroutable_rather_than_refused() {
    let broker = connected().await;
    let publisher = broker.publisher(LapinPublish::default());

    publisher
        .publish(OutgoingMessage::new("", b"x"), None)
        .await
        .expect("the server takes it");
}

// A header whose value is framed as a native property is a short string too: the frame cannot
// carry a longer one, so the publish is refused before anything is routed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_header_the_frame_cannot_carry_is_refused() {
    let broker = connected().await;
    let publisher = broker.publisher(LapinPublish::default());
    let mut headers = HeaderMap::new();
    headers.insert("correlation-id", "c".repeat(300));

    let err = publisher
        .publish(
            OutgoingMessage::new("orders", b"x").with_headers(headers),
            None,
        )
        .await
        .expect_err("a 300-byte correlation id is not a short string");

    assert!(matches!(err, AmqpError::InvalidOptions(_)), "got {err}");
}

// A queue with several consumers is a work queue: each delivery goes to exactly one of them, in
// rotation. A service that scales a handler horizontally must not see every copy processed twice
// in process when a server would hand each message out once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn competing_consumers_share_the_deliveries() {
    let broker = connected().await;
    let mut first = broker
        .subscribe(RabbitQueue::new("shared"))
        .await
        .expect("subscribe first");
    let mut second = broker
        .subscribe(RabbitQueue::new("shared"))
        .await
        .expect("subscribe second");
    let publisher = broker.publisher(LapinPublish::default());

    for payload in [b"m1".as_slice(), b"m2", b"m3", b"m4"] {
        publisher
            .publish(OutgoingMessage::new("shared", payload), None)
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
    let mut first = broker
        .subscribe(RabbitQueue::new("retried"))
        .await
        .expect("subscribe first");
    let mut second = broker
        .subscribe(RabbitQueue::new("retried"))
        .await
        .expect("subscribe second");
    let publisher = broker.publisher(LapinPublish::default());

    publisher
        .publish(OutgoingMessage::new("retried", b"m1"), None)
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
    let mut orders = broker
        .subscribe(RabbitQueue::new("orders"))
        .await
        .expect("subscribe orders");
    let mut events = broker
        .subscribe(RabbitQueue::new("events"))
        .await
        .expect("subscribe events");
    let publisher = broker.publisher(LapinPublish::default());

    publisher
        .publish(OutgoingMessage::new("orders", b"o"), None)
        .await
        .expect("publish o");
    publisher
        .publish(OutgoingMessage::new("events", b"e"), None)
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
    let mut subscriber = broker
        .subscribe(RabbitQueue::new("orders"))
        .await
        .expect("subscribe");
    let publisher = broker.publisher(LapinPublish::default());

    publisher
        .publish(OutgoingMessage::new("orders", b"once"), None)
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
    let mut subscriber = broker
        .subscribe(RabbitQueue::new("orders"))
        .await
        .expect("subscribe");
    let publisher = broker.publisher(LapinPublish::default());

    let mut headers = HeaderMap::new();
    headers.insert("content-type", "application/json");
    headers.insert("correlation-id", "abc-1");
    let outgoing = OutgoingMessage::new("orders", b"{}").with_headers(headers);
    publisher.publish(outgoing, None).await.expect("publish");

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
        .publish(OutgoingMessage::new("events", b"first"), None)
        .await
        .expect("publish first");
    publisher
        .publish(OutgoingMessage::new("events", b"second"), None)
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
    let mut subscriber = broker
        .subscribe(RabbitQueue::new("orders"))
        .await
        .expect("subscribe");
    let publisher = broker.publisher(LapinPublish::default());

    publisher
        .publish(OutgoingMessage::new("orders", b"one"), None)
        .await
        .expect("publish one");
    {
        let mut stream = Box::pin(subscriber.stream());
        assert_eq!(next_payload(&mut stream).await, b"one");
    }

    publisher
        .publish(OutgoingMessage::new("orders", b"two"), None)
        .await
        .expect("publish two");
    let mut stream = Box::pin(subscriber.stream());
    assert_eq!(next_payload(&mut stream).await, b"two");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partition_key_header_is_surfaced() {
    let broker = connected().await;
    let mut sub = broker
        .subscribe(RabbitQueue::new("keyed"))
        .await
        .expect("subscribe");

    let mut headers = HeaderMap::new();
    headers.insert(PARTITION_KEY_HEADER, "tenant-a");
    broker
        .publisher(LapinPublish::default())
        .publish(
            OutgoingMessage::new("keyed", b"payload").with_headers(headers),
            None,
        )
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
    let mut sub = broker
        .subscribe(RabbitQueue::new("unkeyed"))
        .await
        .expect("subscribe");

    broker
        .publisher(LapinPublish::default())
        .publish(OutgoingMessage::new("unkeyed", b"payload"), None)
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

// The in-process transition parses the address as `connect` does, so a broker a service could
// never connect is not one a test connects either.
#[tokio::test]
async fn an_address_connect_refuses_is_refused_in_process() {
    let refused = LapinBroker::new("not a url").connect_in_process().await;

    assert!(
        matches!(refused, Err(AmqpError::Connect(_))),
        "got {refused:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transaction_buffers_until_commit() {
    let broker = connected().await;
    let mut sub = broker
        .subscribe(RabbitQueue::new("tx"))
        .await
        .expect("subscribe");
    let publisher = broker.publisher(LapinPublish::default().confirms());

    publisher.begin_transaction().await.expect("begin");
    publisher
        .publish(OutgoingMessage::new("tx", b"first"), None)
        .await
        .expect("publish first");
    publisher
        .publish(OutgoingMessage::new("tx", b"second"), None)
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
        .publish(OutgoingMessage::new("tx", b"discarded"), None)
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
        .publish(OutgoingMessage::new("tx", b"kept"), None)
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
        .publish(OutgoingMessage::new("orders", b"late"), None)
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

// The descriptor form mounts through the production `SubscriptionSource` impl on `RabbitQueue`,
// which the in-process variant sits under.
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

// The harness connects the production broker in process and installs its coordinator there, so
// a publish drives the reaction to quiescence (every `enqueued` balanced by a `consumed`) before
// it returns.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_app_drives_the_production_app_to_quiescence() {
    let app =
        RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(LapinBroker::new(URI), |b| {
            b.include(ack_order);
            b.include(ack_payment);
        });
    let tb = TestApp::start(app).await.expect("start");

    tb.broker::<LapinBroker>()
        .message(&Order { id: 1 })
        .to("orders")
        .publish()
        .await
        .expect("publish must drive the reaction to quiescence");
    tb.broker::<LapinBroker>()
        .message(&Order { id: 2 })
        .to("payments")
        .publish()
        .await
        .expect("publish must drive the descriptor-mounted reaction to quiescence");

    tb.broker::<LapinBroker>()
        .subscriber("orders")
        .assert_called_once()
        .with(&Order { id: 1 })
        .settled(HandlerOutcome::ack());
    tb.broker::<LapinBroker>()
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

// A handler that binds AMQP delivery fields mounts in process too: the transport reports the
// exchange and routing key the message was published with, and per-subscription delivery tags,
// so the same handler is tested without a server.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ctx_keys_resolve_against_the_in_process_transport() {
    let seen = Seen::default();
    let probe = seen.clone();
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .on_startup(
            move |()| async move { Ok::<_, std::convert::Infallible>(MetadataState { seen }) },
        )
        .with_broker(LapinBroker::new(URI), |b| {
            b.include(record_metadata);
        });
    let tb = TestApp::start(app).await.expect("start");

    tb.broker::<LapinBroker>()
        .message(&Order { id: 3 })
        .to("metadata")
        .publish()
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
        .with_broker(LapinBroker::new(URI), |b| {
            b.include(retry_then_ack);
        });
    let tb = TestApp::start(app).await.expect("start");

    tb.broker::<LapinBroker>()
        .message(&Order { id: 7 })
        .to("retry")
        .publish()
        .await
        .expect("publish must drive the requeue reaction to quiescence");

    tb.broker::<LapinBroker>()
        .subscriber("retry")
        .assert_called(2)
        .settled(HandlerOutcome::ack());

    tb.shutdown().await.expect("shutdown");
}

/// Long enough that nothing comes back until the test advances the clock itself.
const RETRY_DELAY: Duration = Duration::from_secs(5);

/// Stamps every deferred copy with the name of the delivery it answers. The retry position reads
/// the delivery being retried, so its transforms take a `PublishContext` like a reply's.
#[derive(Debug, Clone, Copy)]
struct StampDeferred;

impl<C, Options> PublishTransform<ForReply<C>, Options> for StampDeferred {
    type Destination = Reads;

    fn apply(
        &self,
        out: &mut Outgoing<'_>,
        _options: &mut Option<Options>,
        cx: &PublishContext<'_, C>,
    ) {
        out.headers_mut()
            .insert("x-retried-from", cx.name().to_owned());
    }
}

/// Asks for a delayed redelivery on the first delivery and acks the copy that comes back.
#[subscriber(RabbitQueue::new("orders.deferred"))]
async fn defer_then_ack(order: &Order, ctx: &mut Context<'_>) -> HandlerOutcome {
    let _ = order;
    let attempt = ctx
        .headers()
        .get_str(RETRY_COUNT_HEADER)
        .and_then(|count| count.parse::<u64>().ok())
        .unwrap_or(0);
    if attempt == 0 {
        HandlerOutcome::retry_after(RETRY_DELAY)
    } else {
        HandlerOutcome::ack()
    }
}

// A queue without `.delay(..)` has no delay of its own, so a `retry_after` takes the runtime's
// deferred copy: it leaves through the publisher the mount site bound with `out_retry`, and the
// transforms on that position run on it. What the transform stamps is on the message that reaches
// the handler again.
#[tokio::test(start_paused = true)]
async fn a_transform_on_the_retry_position_stamps_the_deferred_copy() {
    let app =
        RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(LapinBroker::new(URI), |b| {
            b.include(defer_then_ack)
                .out_retry(LapinPublish::default())
                .transform(StampDeferred);
        });
    let tb = TestApp::start(app).await.expect("start");

    tb.broker::<LapinBroker>()
        .message(&Order { id: 11 })
        .to("orders.deferred")
        .publish()
        .await
        .expect("publish must drive the deferred settlement to quiescence");
    tb.broker::<LapinBroker>()
        .subscriber("orders.deferred")
        .assert_called_once()
        .settled(HandlerOutcome::retry_after(RETRY_DELAY));

    tb.advance(RETRY_DELAY).await.expect("the deferred copy");

    tb.broker::<LapinBroker>()
        .subscriber("orders.deferred")
        .assert_called(2)
        .settled(HandlerOutcome::ack());
    tb.broker::<LapinBroker>()
        .published::<Order>("orders.deferred")
        .assert_called(2)
        .with_header("x-retried-from", "orders.deferred");

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
    use ruststream_lapin::DirectReplyTo;

    let app =
        RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(LapinBroker::new(URI), |b| {
            b.include(echo_id)
                .out(Reply, LapinPublish::default())
                .transform(DirectReplyTo);
        });
    let tb = TestApp::start(app).await.expect("start");

    // The addressing a requester puts on its request.
    let mut headers = HeaderMap::new();
    headers.insert("reply-to", "rpc.replies");
    headers.insert("correlation-id", "c-9");
    tb.broker::<LapinBroker>()
        .message(&Order { id: 9 })
        .with_headers(headers)
        .to("rpc.in")
        .publish()
        .await
        .expect("publish must drive the reply to quiescence");
    tb.broker::<LapinBroker>()
        .published::<Order>("rpc.replies")
        .assert_called_once()
        .with(&Order { id: 9 })
        .with_header("correlation-id", "c-9");

    tb.broker::<LapinBroker>()
        .message(&Order { id: 1 })
        .to("rpc.in")
        .publish()
        .await
        .expect("publish must drive the reply to quiescence");
    tb.broker::<LapinBroker>()
        .published::<Order>("rpc.fallback")
        .assert_called_once()
        .with(&Order { id: 1 });

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
        RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(LapinBroker::new(URI), |b| {
            b.include(confirm_order).out(Reply, LapinPublish::default());
        });
    let tb = TestApp::start(app).await.expect("start");

    tb.broker::<LapinBroker>()
        .message(&Order { id: 4 })
        .to("orders.declared")
        .publish()
        .await
        .expect("publish must drive the reply to quiescence");

    tb.broker::<LapinBroker>()
        .subscriber("orders.declared")
        .assert_called_once();
    tb.broker::<LapinBroker>()
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
        RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(LapinBroker::new(URI), |b| {
            b.include(receipt_for).out(Reply, LapinPublish::default());
        });
    let tb = TestApp::start(app).await.expect("start");

    tb.broker::<LapinBroker>()
        .message(&Order { id: 5 })
        .to("orders.mounted")
        .publish()
        .await
        .expect("publish must drive the reply to quiescence");

    tb.broker::<LapinBroker>()
        .subscriber("orders.mounted")
        .assert_called_once();
    tb.broker::<LapinBroker>()
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
    let mut subscriber = broker
        .subscribe(RabbitQueue::new("batches"))
        .await
        .expect("subscribe");
    let publisher = broker.publisher(LapinPublish::default());
    for payload in [b"p1".as_slice(), b"p2", b"p3", b"p4", b"p5"] {
        publisher
            .publish(OutgoingMessage::new("batches", payload), None)
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
        RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(LapinBroker::new(URI), |b| {
            b.include(settle_batch.batch(nonzero!(4)));
        });
    let tb = TestApp::start(app).await.expect("start");

    for id in 1..=2 {
        tb.broker::<LapinBroker>()
            .message(&Order { id })
            .to("batches.settled")
            .publish()
            .await
            .expect("publish must drive the batch to quiescence");
    }

    tb.broker::<LapinBroker>()
        .subscriber("batches.settled")
        .assert_batch_sizes(&[1, 1])
        .settled(HandlerOutcome::ack());

    tb.shutdown().await.expect("shutdown");
}

// --- What the server refuses, refused in process the same way. ---

// RabbitMQ 4 refuses a transient queue that is not exclusive, so a broker that declares its
// topology cannot open one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_transient_shared_queue_is_refused_where_the_topology_is_declared() {
    let broker = declaring().await;

    let refused = broker
        .subscribe(RabbitQueue::new("orders.transient").durable(false))
        .await;

    assert!(
        matches!(refused, Err(AmqpError::Declare(_))),
        "got {refused:?}"
    );
}

// The server permits no operation on the default exchange, a binding included.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_binding_on_the_default_exchange_is_refused() {
    let broker = declaring().await;

    let refused = broker
        .subscribe(RabbitQueue::new("orders").bind(RabbitExchange::direct(""), "orders"))
        .await;

    assert!(
        matches!(refused, Err(AmqpError::Declare(_))),
        "got {refused:?}"
    );
}

// A queue exists once, with the settings its first declaration gave it; a second declaration
// that asks for other ones is refused as inequivalent, and an equal one is not.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_queue_declared_again_with_other_settings_is_refused() {
    let broker = declaring().await;
    let _first = broker
        .subscribe(RabbitQueue::new("orders.shared"))
        .await
        .expect("the first declaration");
    let _again = broker
        .subscribe(RabbitQueue::new("orders.shared"))
        .await
        .expect("the same declaration again");

    let refused = broker
        .subscribe(RabbitQuorumQueue::new("orders.shared"))
        .await;

    assert!(
        matches!(refused, Err(AmqpError::Declare(_))),
        "got {refused:?}"
    );
}

// An argument a quorum queue does not take fails its declaration.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_quorum_queue_refuses_a_priority_argument() {
    let broker = declaring().await;

    let refused = broker
        .subscribe(
            RabbitQuorumQueue::new("orders.prioritised")
                .argument("x-max-priority", AMQPValue::LongLongInt(5)),
        )
        .await;

    assert!(
        matches!(refused, Err(AmqpError::Declare(_))),
        "got {refused:?}"
    );
}

// A delivery settles on the channel it came on, and a closed connection takes that channel with
// it: the acknowledgement fails rather than claiming a settlement that never happened.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn settling_after_the_connection_closed_is_refused() {
    let broker = connected().await;
    let mut subscriber = broker
        .subscribe(RabbitQueue::new("orders"))
        .await
        .expect("subscribe");
    broker
        .publisher(LapinPublish::default())
        .publish(OutgoingMessage::new("orders", b"o"), None)
        .await
        .expect("publish");
    let msg = tokio::time::timeout(WAIT, Box::pin(subscriber.stream()).next())
        .await
        .expect("delivery within timeout")
        .expect("stream has next")
        .expect("delivery ok");

    broker.shutdown().await.expect("shutdown");

    let refused = msg.ack().await;
    assert!(
        matches!(refused, Err(AckError::Broker(_))),
        "got {refused:?}"
    );
}

// --- Exchanges route through the bindings the descriptors describe. ---

/// The next delivery on `subscriber`, or `None` when nothing arrives within a short wait.
async fn delivered(subscriber: &mut LapinSubscriber) -> Option<Vec<u8>> {
    let next = tokio::time::timeout(
        Duration::from_millis(50),
        Box::pin(subscriber.stream()).next(),
    )
    .await
    .ok()??
    .expect("delivery ok");
    let payload = next.payload().to_vec();
    next.ack().await.expect("ack");
    Some(payload)
}

// A topic exchange reaches the queues whose binding pattern matches the routing key, and no queue
// by its name alone: a message to `events` under `order.created` does not land in a queue named
// `order.created` that no binding connects.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_topic_exchange_routes_by_the_binding_pattern() {
    let broker = connected().await;
    let events = RabbitExchange::topic("events");
    let mut audit = broker
        .subscribe(RabbitQueue::new("audit").bind(events.clone(), "order.*"))
        .await
        .expect("subscribe audit");
    let mut shipping = broker
        .subscribe(RabbitQueue::new("shipping").bind(events, "order.shipped"))
        .await
        .expect("subscribe shipping");
    let mut named_like_the_key = broker
        .subscribe(RabbitQueue::new("order.created"))
        .await
        .expect("subscribe a queue named like the key");

    broker
        .publisher(LapinPublish::default().exchange("events"))
        .publish(OutgoingMessage::new("order.created", b"created"), None)
        .await
        .expect("publish");

    assert_eq!(
        delivered(&mut audit).await.as_deref(),
        Some(&b"created"[..])
    );
    assert_eq!(delivered(&mut shipping).await, None);
    assert_eq!(delivered(&mut named_like_the_key).await, None);
}

// A fanout exchange reaches every bound queue, whatever the routing key.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fanout_exchange_reaches_every_bound_queue() {
    let broker = connected().await;
    let fanout = RabbitExchange::fanout("broadcast");
    let mut first = broker
        .subscribe(RabbitQueue::new("first").bind(fanout.clone(), ""))
        .await
        .expect("subscribe first");
    let mut second = broker
        .subscribe(RabbitQueue::new("second").bind(fanout, ""))
        .await
        .expect("subscribe second");

    broker
        .publisher(LapinPublish::default().exchange("broadcast"))
        .publish(OutgoingMessage::new("anything", b"all"), None)
        .await
        .expect("publish");

    assert_eq!(delivered(&mut first).await.as_deref(), Some(&b"all"[..]));
    assert_eq!(delivered(&mut second).await.as_deref(), Some(&b"all"[..]));
}

// A headers exchange routes on the binding's arguments against the message's header table.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_headers_exchange_routes_on_the_binding_arguments() {
    let broker = connected().await;
    let mut arguments = FieldTable::default();
    arguments.insert("x-match".into(), AMQPValue::LongString("all".into()));
    arguments.insert("region".into(), AMQPValue::LongString("eu".into()));
    let mut eu = broker
        .subscribe(RabbitQueue::new("eu").bind_with(
            RabbitExchange::headers("by-region"),
            "",
            arguments,
        ))
        .await
        .expect("subscribe");
    let publisher = broker.publisher(LapinPublish::default().exchange("by-region"));

    for (region, payload) in [("us", b"us".as_slice()), ("eu", b"eu")] {
        let mut headers = HeaderMap::new();
        headers.insert("region", region);
        publisher
            .publish(
                OutgoingMessage::new("", payload).with_headers(headers),
                None,
            )
            .await
            .expect("publish");
    }

    assert_eq!(delivered(&mut eu).await.as_deref(), Some(&b"eu"[..]));
    assert_eq!(delivered(&mut eu).await, None);
}
