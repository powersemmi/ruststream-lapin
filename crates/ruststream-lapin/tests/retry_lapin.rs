//! What a registration declares about its retries, driven in process.
//!
//! Both queue descriptors move a delivery to its dead-letter route themselves, so the declaration
//! becomes the queue's own arguments and the service publishes no copy. A classic queue takes the
//! destination and refuses a cap, because it counts nothing. A quorum queue counts the deliveries
//! too, so it takes both steps and carries a spent delivery away at the cap. A bare queue name
//! carries no arguments at all, and refuses both.
//!
//! The transport's own half is here too: a descriptor that names a waiting queue makes the delay
//! the broker's, and the copy it releases is a new message counted from zero.

#![cfg(feature = "testing")]

use std::convert::Infallible;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::StreamExt;
use ruststream::runtime::RETRY_COUNT_HEADER;
use ruststream::testing::{InProcess, TestApp, TestError};
use ruststream::{ConnectedBroker, FromRef, OutgoingMessage, Subscriber, SubscriptionSource};
use ruststream_lapin::prelude::*;
use ruststream_lapin::{ConnectedLapinBroker, LapinMessage, LapinSubscriber};
use serde::{Deserialize, Serialize};

use crate::live;

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

/// Sets aside every order it is handed: the queue's dead-letter route takes it from there.
#[subscriber(RabbitQueue::new("orders.dropped"))]
async fn set_aside(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::drop()
}

/// The same handler over the bare-name form, which carries no queue arguments.
#[subscriber("orders.dropped.by-name")]
async fn set_aside_by_name(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::drop()
}

/// Never ready: every delivery asks to come back.
#[subscriber(RabbitQueue::new("orders.classic"))]
async fn never_ready_on_a_classic_queue(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::retry()
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

// A classic queue dead-letters a dropped delivery itself: the destination the mount site declared
// is the queue's own route, so the message reaches the dead-letter queue with nothing published by
// the service.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dropped_delivery_leaves_for_the_declared_dead_letter_queue() {
    let broker = LapinBroker::new(URI).declare_topology(true);
    let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(set_aside).dead_letter(DEAD);
        b.include(bury);
    });
    let tb = TestApp::start(app).await.expect("start");

    tb.broker::<LapinBroker>()
        .message(&Order { id: 1 })
        .to("orders.dropped")
        .publish()
        .await
        .expect("publish drives the delivery to quiescence");

    tb.broker::<LapinBroker>()
        .subscriber("orders.dropped")
        .assert_called_once()
        .settled(HandlerOutcome::drop());
    tb.broker::<LapinBroker>()
        .subscriber(DEAD)
        .assert_called_once()
        .with(&Order { id: 1 });
    tb.broker::<LapinBroker>()
        .published::<Order>(DEAD)
        .assert_not_called();

    tb.shutdown().await.expect("shutdown");
}

// A classic queue keeps no delivery count, so nothing would apply a cap declared on it: the
// registration does not start, and the refusal names the queue that has one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cap_on_a_classic_queue_refuses_to_start() {
    let broker = LapinBroker::new(URI).declare_topology(true);
    let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(never_ready_on_a_classic_queue)
            .max_attempts(nonzero!(ATTEMPTS))
            .dead_letter(DEAD);
    });

    let err = TestApp::start(app)
        .await
        .expect_err("a classic queue has nothing to apply a cap with");
    let message = err.to_string();
    assert!(message.contains("orders.classic"), "{message}");
    assert!(message.contains("RabbitQuorumQueue"), "{message}");
}

// A bare queue name carries no queue arguments, so a dead-letter destination declared on it would
// reach no route: the registration does not start, and the refusal names the descriptor to use.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_declaration_on_a_bare_queue_name_refuses_to_start() {
    let app =
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(LapinBroker::new(URI), |b| {
            b.include(set_aside_by_name).dead_letter(DEAD);
        });

    let err = TestApp::start(app)
        .await
        .expect_err("a bare name has no arguments to carry the route");
    let message = err.to_string();
    assert!(message.contains("orders.dropped.by-name"), "{message}");
    assert!(message.contains("RabbitQueue::new"), "{message}");
}

/// Two registrations of one queue, each declaring a dead-letter route of its own.
#[subscriber(RabbitQueue::new("orders.routed"))]
async fn set_aside_here(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::drop()
}

#[subscriber(RabbitQueue::new("orders.routed"))]
async fn set_aside_there(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::drop()
}

// A queue carries one dead-letter route: the second declaration names another, which the server
// refuses as inequivalent, and the refusal names the queue and both sets of arguments.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_second_route_for_one_queue_refuses_to_start() {
    let broker = LapinBroker::new(URI).declare_topology(true);
    let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(set_aside_here).dead_letter(DEAD);
        b.include(set_aside_there).dead_letter("orders.elsewhere");
    });

    let err = TestApp::start(app)
        .await
        .expect_err("one queue cannot carry two dead-letter routes");
    let message = err.to_string();
    assert!(message.contains("orders.routed"), "{message}");
    assert!(message.contains(DEAD), "{message}");
    assert!(message.contains("orders.elsewhere"), "{message}");
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
            async move { Ok::<_, Infallible>(seen) }
        })
        .with_broker(LapinBroker::new(URI), |b| {
            b.include(record_the_count);
        });
    let tb = TestApp::start(app).await.expect("start");

    tb.broker::<LapinBroker>()
        .message(&Order { id: 5 })
        .to("orders.counted")
        .publish()
        .await
        .expect("publish drives the first delivery to quiescence");
    for _ in 1..ATTEMPTS {
        tb.advance(RETRY_DELAY)
            .await
            .expect("the waiting queue releases the copy");
    }

    assert_eq!(
        *recorded.lock().expect("counts mutex poisoned"),
        vec![None, Some("1".to_owned()), Some("2".to_owned())],
    );

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

// --- Competing consumers against a live broker. ---

#[subscriber(RabbitQueue::new("orders.shared").auto_delete(true))]
async fn first_of_two(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

#[subscriber(RabbitQueue::new("orders.shared").auto_delete(true))]
async fn second_of_two(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

// RabbitMQ picks which of a queue's consumers takes a message, out of the harness's sight, so a
// live test with two subscriptions of one queue stops with the reason instead of timing out on
// the consumer the server did not pick.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_live_test_refuses_two_consumers_of_one_queue() {
    let Some(url) = live::url("AMQP_TEST_URL") else {
        return;
    };
    let run = tokio::spawn(async move {
        let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
            LapinBroker::new(url).declare_topology(true),
            |b| {
                b.include(first_of_two);
                b.include(second_of_two);
            },
        );
        let tb = TestApp::start_live(app)
            .await
            .expect("start against the stand");
        tb.broker::<LapinBroker>()
            .message(&Order { id: 7 })
            .to("orders.shared")
            .publish()
            .await
    });
    let panic = run
        .await
        .expect_err("the harness cannot tell which consumer owes the message")
        .into_panic();
    let message = panic.downcast_ref::<String>().map_or("", String::as_str);
    assert!(
        message.contains("\"orders.shared\" has 2 subscriptions"),
        "{message}"
    );
}
