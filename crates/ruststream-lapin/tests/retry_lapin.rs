//! What a registration declares about its retries, driven in process.
//!
//! The two queue descriptors answer it differently, and both are here. A classic queue counts
//! nothing, so the copies are the service's and the declaration is the runtime's to apply: the cap
//! counts the deliveries and the dead-letter destination takes the spent one, the same on a
//! descriptor as on a bare queue name. A quorum queue counts the deliveries itself, so the same
//! two steps become its own arguments and the queue carries a spent delivery away with nothing
//! published by the service.
//!
//! The transport's own half is here too: a descriptor that names a waiting queue makes the delay
//! the broker's, and the copy it releases is a new message counted from zero.

#![cfg(feature = "testing")]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::StreamExt;
use ruststream::runtime::RETRY_COUNT_HEADER;
use ruststream::testing::{InProcess, TestApp, TestError};
use ruststream::{
    ConnectedBroker, FromRef, IncomingMessage, OutgoingMessage, Publisher, Subscriber,
    SubscriptionSource,
};
use ruststream_lapin::prelude::*;
use ruststream_lapin::{ConnectedLapinBroker, LapinMessage, LapinSubscriber};
use serde::{Deserialize, Serialize};

mod live;

/// The address the service's broker is built with; the in-process mode dials nothing.
const URI: &str = "amqp://localhost:5672";

/// Long enough that nothing comes back until the test advances the clock itself.
const RETRY_DELAY: Duration = Duration::from_secs(5);

/// Three deliveries and then the dead letter, which is small enough to read in the assertions.
const ATTEMPTS: u32 = 3;

/// Where a spent delivery goes. A destination is a routing key here, so the copy lands in the
/// queue of that name.
const DEAD: &str = "orders.dead";

#[derive(Debug, PartialEq, Outgoing, Serialize, Deserialize)]
struct Order {
    id: u64,
}

/// Never ready: every delivery asks to come back later, which is what runs a cap down.
#[subscriber(RabbitQueue::new("orders.capped"))]
async fn never_ready(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::retry_after(RETRY_DELAY)
}

/// The same handler over the bare-name form, whose copy path comes from the broker rather than
/// from a descriptor.
#[subscriber("orders.capped.by-name")]
async fn never_ready_by_name(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::retry_after(RETRY_DELAY)
}

/// The same handler over a quorum queue, retried immediately: the queue counts the deliveries and
/// carries the spent one away itself, so the runtime publishes no copy.
#[subscriber(RabbitQuorumQueue::new("orders.quorum"))]
async fn never_ready_on_a_quorum_queue(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::retry()
}

/// The same handler over a descriptor that names a waiting queue: the delay is the broker's
/// there, so the runtime publishes no copy of its own.
#[subscriber(RabbitQueue::new("orders.waiting").delay(Delay::dlx_ttl()))]
async fn never_ready_on_a_waiting_queue(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::retry_after(RETRY_DELAY)
}

/// Runs one registration down to its cap: the first delivery, then one more per elapsed delay.
async fn exhaust<S: Send + Sync + 'static>(tb: &TestApp<S>, queue: &str, order: &Order) {
    tb.broker::<LapinBroker>()
        .message(order)
        .to(queue)
        .publish()
        .await
        .expect("publish drives the first delivery to quiescence");
    for _ in 1..ATTEMPTS {
        tb.advance(RETRY_DELAY).await.expect("the deferred copy");
    }
}

// The cap is the registration's word, and it reads the same on a descriptor as it does anywhere
// else in the family: three deliveries, then the delivery leaves for the dead-letter queue instead
// of coming back a fourth time.
#[tokio::test(start_paused = true)]
async fn a_declared_cap_ends_a_retry_loop_on_a_descriptor() {
    let app =
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(LapinBroker::new(URI), |b| {
            b.include(never_ready)
                .max_attempts(nonzero!(ATTEMPTS))
                .dead_letter(DEAD);
        });
    let tb = TestApp::start(app).await.expect("start");

    exhaust(&tb, "orders.capped", &Order { id: 1 }).await;

    tb.broker::<LapinBroker>()
        .subscriber("orders.capped")
        .assert_called(ATTEMPTS as usize);
    tb.broker::<LapinBroker>()
        .published::<Order>(DEAD)
        .assert_called_once()
        .decoded_as::<Order>()
        .with(&Order { id: 1 });

    tb.shutdown().await.expect("shutdown");
}

// The bare-name form takes the queue's own copy path, so the same two steps end the same loop.
#[tokio::test(start_paused = true)]
async fn a_declared_cap_ends_a_retry_loop_on_a_bare_queue_name() {
    let app =
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(LapinBroker::new(URI), |b| {
            b.include(never_ready_by_name)
                .max_attempts(nonzero!(ATTEMPTS))
                .dead_letter(DEAD);
        });
    let tb = TestApp::start(app).await.expect("start");

    exhaust(&tb, "orders.capped.by-name", &Order { id: 2 }).await;

    tb.broker::<LapinBroker>()
        .subscriber("orders.capped.by-name")
        .assert_called(ATTEMPTS as usize);
    tb.broker::<LapinBroker>()
        .published::<Order>(DEAD)
        .assert_called_once()
        .decoded_as::<Order>()
        .with(&Order { id: 2 });

    tb.shutdown().await.expect("shutdown");
}

// A waiting queue is the broker's own delay, so the delayed copy is the transport's and the
// service publishes nothing: the queue's publish log still holds the one message that started it.
#[tokio::test(start_paused = true)]
async fn a_waiting_queue_keeps_the_delayed_copy_off_the_service() {
    let app =
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(LapinBroker::new(URI), |b| {
            b.include(never_ready_on_a_waiting_queue);
        });
    let tb = TestApp::start(app).await.expect("start");

    tb.broker::<LapinBroker>()
        .message(&Order { id: 3 })
        .to("orders.waiting")
        .publish()
        .await
        .expect("publish drives the first delivery to quiescence");
    tb.advance(RETRY_DELAY)
        .await
        .expect("the waiting queue releases the copy");

    tb.broker::<LapinBroker>()
        .subscriber("orders.waiting")
        .assert_called(2);
    tb.broker::<LapinBroker>()
        .published::<Order>("orders.waiting")
        .assert_called_once();

    tb.shutdown().await.expect("shutdown");
}

/// Takes whatever reaches the dead-letter queue, so a test sees what the queue carried there.
#[subscriber("orders.dead")]
async fn bury(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

// A quorum queue ends its own retry loop: the cap and the destination the mount site declared are
// the queue's arguments, so the deliveries run out and the message leaves for the dead-letter
// queue with nothing published by the service. The queue has those arguments only on a broker
// that declares it, which is the broker this service is built on.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_quorum_queue_ends_the_loop_itself() {
    let broker = LapinBroker::new(URI).declare_topology(true);
    let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(never_ready_on_a_quorum_queue)
            .max_attempts(nonzero!(ATTEMPTS))
            .dead_letter(DEAD);
        b.include(bury);
    });
    let tb = TestApp::start(app).await.expect("start");

    tb.broker::<LapinBroker>()
        .message(&Order { id: 7 })
        .to("orders.quorum")
        .publish()
        .await
        .expect("publish drives the loop to quiescence");

    tb.broker::<LapinBroker>()
        .subscriber("orders.quorum")
        .assert_called(ATTEMPTS as usize);
    // The queue moved the message, and the service published nothing: the dead letter is read
    // where it arrived.
    tb.broker::<LapinBroker>()
        .subscriber(DEAD)
        .assert_called_once()
        .with(&Order { id: 7 });
    tb.broker::<LapinBroker>()
        .published::<Order>(DEAD)
        .assert_not_called();

    tb.shutdown().await.expect("shutdown");
}

// The same registration on a broker that declares nothing does not start: the queue on the server
// carries whatever arguments its owner gave it, so a cap declared here would apply nowhere. The
// in-process mode reads the broker's own setting, so the start fails in a test where it fails on
// the server.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_quorum_declaration_on_a_broker_that_declares_nothing_refuses_to_start() {
    let app =
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(LapinBroker::new(URI), |b| {
            b.include(never_ready_on_a_quorum_queue)
                .max_attempts(nonzero!(ATTEMPTS))
                .dead_letter(DEAD);
        });

    match TestApp::start(app).await {
        Err(TestError::Subscribe(source)) => {
            let message = source.to_string();
            assert!(message.contains("declare_topology(true)"), "{message}");
        }
        other => panic!("expected the start refused, got {:?}", other.map(|_| ())),
    }
}

// Half a declaration is refused before the subscription opens: a quorum queue carries a spent
// delivery away only when it knows both when and where, and a limit with no destination drops the
// message instead.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn half_a_declaration_refuses_to_start() {
    let app =
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(LapinBroker::new(URI), |b| {
            b.include(never_ready_on_a_quorum_queue)
                .max_attempts(nonzero!(ATTEMPTS));
        });

    let err = TestApp::start(app)
        .await
        .expect_err("a half declaration cannot open the subscription");
    let message = err.to_string();
    assert!(message.contains("orders.quorum"), "{message}");
    assert!(message.contains("dead_letter"), "{message}");
}

/// Subscribes to `def` and publishes one message to it.
async fn subscribe_and_publish<S>(connected: &ConnectedLapinBroker, def: S) -> LapinSubscriber
where
    S: SubscriptionSource<ConnectedLapinBroker, Subscriber = LapinSubscriber>,
{
    let queue = def.name().to_owned();
    let subscriber = def
        .subscribe(connected)
        .await
        .expect("the descriptor opens against the connected form");
    connected
        .publisher(Publish::default())
        .publish(OutgoingMessage::new(&queue, b"{}".as_slice()), None)
        .await
        .expect("publish");
    subscriber
}

/// The next delivery on `subscriber`. The stream borrows it, so each call takes its own.
async fn next(subscriber: &mut LapinSubscriber) -> LapinMessage {
    Box::pin(subscriber.stream())
        .next()
        .await
        .expect("a delivery")
        .expect("a delivery")
}

// A classic queue keeps no counter, so a delivery off one says nothing about how far the message
// has come, and the framework's own header is the whole count there.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_classic_delivery_carries_no_count() {
    let connected = LapinBroker::new(URI)
        .connect_in_process()
        .await
        .expect("connect");

    let mut subscriber =
        subscribe_and_publish(&connected, RabbitQueue::new("orders.uncounted")).await;
    let first = next(&mut subscriber).await;
    assert_eq!(first.redelivery_count(), None);
    first.nack(true).await.expect("requeue");

    assert_eq!(next(&mut subscriber).await.redelivery_count(), None);

    connected.shutdown().await.expect("shutdown");
}

// A quorum queue counts a rejected delivery, and this crate settles a handler's retry with a
// rejection, so the redelivery reports the second of the message's deliveries.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_quorum_delivery_counts_the_deliveries_it_has_spent() {
    let connected = LapinBroker::new(URI)
        .connect_in_process()
        .await
        .expect("connect");

    let mut subscriber =
        subscribe_and_publish(&connected, RabbitQuorumQueue::new("orders.counted")).await;
    let first = next(&mut subscriber).await;
    assert_eq!(first.redelivery_count(), None);
    first.nack(true).await.expect("requeue");

    let second = next(&mut subscriber).await;
    assert_eq!(second.redelivery_count(), Some(2));
    second.nack(true).await.expect("requeue");

    assert_eq!(next(&mut subscriber).await.redelivery_count(), Some(3));

    connected.shutdown().await.expect("shutdown");
}

/// What each delivery of the counted queue reported as its retry count, in order.
type Counts = Arc<Mutex<Vec<Option<String>>>>;

#[derive(Clone, Default, FromRef)]
struct Seen {
    counts: Counts,
}

/// Never ready, and it writes down what the delivery said about its own attempts.
#[subscriber(RabbitQueue::new("orders.counted").delay(Delay::dlx_ttl()))]
async fn record_the_count(
    order: &Order,
    ctx: &mut Context<'_>,
    State(counts): State<Counts>,
) -> HandlerOutcome {
    let _ = order.id;
    counts
        .lock()
        .expect("counts mutex poisoned")
        .push(ctx.headers().get_str(RETRY_COUNT_HEADER).map(str::to_owned));
    HandlerOutcome::retry_after(RETRY_DELAY)
}

// The waiting queue releases a new message, so the only record of the round is the framework's
// count, and the copy carries it one higher every time.
#[tokio::test(start_paused = true)]
async fn every_copy_the_waiting_queue_releases_counts_one_more_attempt() {
    let seen = Seen::default();
    let recorded = Arc::clone(&seen.counts);
    let app = RustStream::new(AppInfo::new("orders", "0.1.0"))
        .on_startup(move |()| {
            let seen = seen;
            async move { Ok::<_, std::convert::Infallible>(seen) }
        })
        .with_broker(LapinBroker::new(URI), |b| {
            b.include(record_the_count);
        });
    let tb = TestApp::start(app).await.expect("start");

    exhaust(&tb, "orders.counted", &Order { id: 5 }).await;

    assert_eq!(
        *recorded.lock().expect("counts mutex poisoned"),
        vec![None, Some("1".to_owned()), Some("2".to_owned())],
    );

    tb.shutdown().await.expect("shutdown");
}

// The cap over a waiting queue. The broker releases the copy, so the queue counts nothing and the
// framework's count on the copy is the only record of the round: the runtime reads it on the
// delayed path too, and the cap ends the loop at the dead-letter queue.
#[tokio::test(start_paused = true)]
async fn a_declared_cap_ends_a_delayed_retry_loop() {
    let app =
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(LapinBroker::new(URI), |b| {
            b.include(never_ready_on_a_waiting_queue)
                .max_attempts(nonzero!(ATTEMPTS))
                .dead_letter(DEAD);
        });
    let tb = TestApp::start(app).await.expect("start");

    exhaust(&tb, "orders.waiting", &Order { id: 6 }).await;

    tb.broker::<LapinBroker>()
        .subscriber("orders.waiting")
        .assert_called(ATTEMPTS as usize);
    tb.broker::<LapinBroker>()
        .published::<Order>(DEAD)
        .assert_called_once()
        .decoded_as::<Order>()
        .with(&Order { id: 6 });

    tb.shutdown().await.expect("shutdown");
}

// --- One test body, in process and against a live broker. ---

/// Long enough for the waiting queue on a live server to hold the copy, short enough for a live run.
const WAITED: Duration = Duration::from_secs(1);

/// Asks for the first delivery back after [`WAITED`], and acknowledges the copy the waiting queue
/// releases, which carries the framework's retry count.
#[subscriber(RabbitQueue::new("orders.dual").auto_delete(true).delay(Delay::dlx_ttl()))]
async fn defer_once(order: &Order, ctx: &mut Context<'_>) -> HandlerOutcome {
    let _ = order.id;
    if ctx.headers().get_str(RETRY_COUNT_HEADER).is_none() {
        return HandlerOutcome::retry_after(WAITED);
    }
    HandlerOutcome::ack()
}

/// The app `main` runs, on the broker it is handed: the production builder, unchanged in either
/// mode.
fn dual_app(broker: LapinBroker) -> RustStream {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        broker.declare_topology(true),
        |b| {
            b.include(defer_once);
        },
    )
}

/// The body both modes run: the first delivery asks to come back, the waiting queue holds the
/// copy for the delay, and the copy is handled and acknowledged.
async fn a_delayed_redelivery_comes_back(tb: TestApp<()>) {
    tb.broker::<LapinBroker>()
        .message(&Order { id: 42 })
        .to("orders.dual")
        .publish()
        .await
        .expect("publish drives the first delivery to its settlement");
    tb.broker::<LapinBroker>()
        .subscriber("orders.dual")
        .assert_called_once()
        .settled(HandlerOutcome::retry_after(WAITED));

    tb.advance(WAITED)
        .await
        .expect("the waiting queue releases the copy");

    tb.broker::<LapinBroker>()
        .subscriber("orders.dual")
        .assert_called(2)
        .settled(HandlerOutcome::ack());
    tb.broker::<LapinBroker>()
        .published::<Order>("orders.dual")
        .assert_called_once()
        .with(&Order { id: 42 });

    tb.shutdown().await.expect("shutdown");
}

#[tokio::test(start_paused = true)]
async fn a_delayed_redelivery_comes_back_in_process() {
    let tb = TestApp::start(dual_app(LapinBroker::new(URI)))
        .await
        .expect("start");
    a_delayed_redelivery_comes_back(tb).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_delayed_redelivery_comes_back_live() {
    let Some(url) = live::url("AMQP_TEST_URL") else {
        return;
    };
    let tb = TestApp::start_live(dual_app(LapinBroker::new(url)))
        .await
        .expect("start against the stand");
    a_delayed_redelivery_comes_back(tb).await;
}
