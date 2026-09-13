//! What a registration declares about its retries, driven in process.
//!
//! A queue addresses its own redeliveries, so the copies are the service's and the declaration is
//! the runtime's to apply: the cap counts the deliveries and the dead-letter destination takes the
//! spent one. The same two steps read the same on a descriptor and on a bare queue name.
//!
//! The transport's own half is here too: a descriptor that names a waiting queue makes the delay
//! the broker's, and no delivery carries a count, because a server counts a delivery whose
//! consumer went away and nothing under the harness can do that.

#![cfg(feature = "testing")]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::StreamExt;
use ruststream::runtime::RETRY_COUNT_HEADER;
use ruststream::testing::TestApp;
use ruststream::{
    Broker, ConnectedBroker, FromRef, IncomingMessage, OutgoingMessage, Publisher, Subscriber,
    SubscriptionSource,
};
use ruststream_lapin::prelude::*;
use ruststream_lapin::testing::{
    ConnectedLapinTestBroker, LapinTestBroker, LapinTestMessage, LapinTestSubscriber,
};
use serde::{Deserialize, Serialize};

/// Long enough that nothing comes back until the test advances the clock itself.
const RETRY_DELAY: Duration = Duration::from_secs(5);

/// Three deliveries and then the dead letter, which is small enough to read in the assertions.
const ATTEMPTS: u32 = 3;

/// Where a spent delivery goes. A destination is a routing key here, so the copy lands in the
/// queue of that name.
const DEAD: &str = "orders.dead";

#[derive(Debug, PartialEq, Serialize, Deserialize)]
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

/// The same handler over a descriptor that names a waiting queue: the delay is the broker's
/// there, so the runtime publishes no copy of its own.
#[subscriber(RabbitQueue::new("orders.waiting").delay(Delay::dlx_ttl()))]
async fn never_ready_on_a_waiting_queue(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::retry_after(RETRY_DELAY)
}

/// Runs one registration down to its cap: the first delivery, then one more per elapsed delay.
async fn exhaust<S: Send + Sync + 'static>(tb: &TestApp<S>, queue: &str, order: &Order) {
    tb.broker::<LapinTestBroker>()
        .publish(queue, order)
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
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(LapinTestBroker::new(), |b| {
            b.include(never_ready)
                .max_attempts(nonzero!(ATTEMPTS))
                .dead_letter(DEAD);
        });
    let tb = TestApp::start(app).await.expect("start");

    exhaust(&tb, "orders.capped", &Order { id: 1 }).await;

    tb.broker::<LapinTestBroker>()
        .subscriber("orders.capped")
        .assert_called(ATTEMPTS as usize);
    tb.broker::<LapinTestBroker>()
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
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(LapinTestBroker::new(), |b| {
            b.include(never_ready_by_name)
                .max_attempts(nonzero!(ATTEMPTS))
                .dead_letter(DEAD);
        });
    let tb = TestApp::start(app).await.expect("start");

    exhaust(&tb, "orders.capped.by-name", &Order { id: 2 }).await;

    tb.broker::<LapinTestBroker>()
        .subscriber("orders.capped.by-name")
        .assert_called(ATTEMPTS as usize);
    tb.broker::<LapinTestBroker>()
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
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(LapinTestBroker::new(), |b| {
            b.include(never_ready_on_a_waiting_queue);
        });
    let tb = TestApp::start(app).await.expect("start");

    tb.broker::<LapinTestBroker>()
        .publish("orders.waiting", &Order { id: 3 })
        .await
        .expect("publish drives the first delivery to quiescence");
    tb.advance(RETRY_DELAY)
        .await
        .expect("the waiting queue releases the copy");

    tb.broker::<LapinTestBroker>()
        .subscriber("orders.waiting")
        .assert_called(2);
    tb.broker::<LapinTestBroker>()
        .published::<Order>("orders.waiting")
        .assert_called_once();

    tb.shutdown().await.expect("shutdown");
}

/// Subscribes to `def` and publishes one message to it.
async fn subscribe_and_publish(
    connected: &ConnectedLapinTestBroker,
    def: RabbitQueue,
) -> LapinTestSubscriber {
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
async fn next(subscriber: &mut LapinTestSubscriber) -> LapinTestMessage {
    Box::pin(subscriber.stream())
        .next()
        .await
        .expect("a delivery")
        .expect("a delivery")
}

// The transport counts no deliveries, whatever queue type the descriptor names: on a server the
// count rises when a delivery's consumer goes away without settling it, and a handler under the
// harness always settles. A count invented here would take the runtime down the broker's requeue
// path where a server sends it down the copy path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_delivery_carries_a_count() {
    let connected = LapinTestBroker::new().connect().await.expect("connect");
    let def = RabbitQueue::new("orders.counted").queue_type(QueueType::Quorum);

    let mut subscriber = subscribe_and_publish(&connected, def).await;
    let first = next(&mut subscriber).await;
    assert_eq!(first.redelivery_count(), None);
    first.nack(true).await.expect("requeue");

    assert_eq!(next(&mut subscriber).await.redelivery_count(), None);

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
        .with_broker(LapinTestBroker::new(), |b| {
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

// The cap over a waiting queue, which the copy's count is what makes countable. The core reads
// that count on the native delayed path from the release after 0.7.0-rc.5; until this crate's
// floor names it, the delivery keeps coming back and the assertions below cannot hold.
#[ignore = "needs the core after 0.7.0-rc.5, which reads the retry count on the delayed path"]
#[tokio::test(start_paused = true)]
async fn a_declared_cap_ends_a_delayed_retry_loop() {
    let app =
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(LapinTestBroker::new(), |b| {
            b.include(never_ready_on_a_waiting_queue)
                .max_attempts(nonzero!(ATTEMPTS))
                .dead_letter(DEAD);
        });
    let tb = TestApp::start(app).await.expect("start");

    exhaust(&tb, "orders.waiting", &Order { id: 6 }).await;

    tb.broker::<LapinTestBroker>()
        .subscriber("orders.waiting")
        .assert_called(ATTEMPTS as usize);
    tb.broker::<LapinTestBroker>()
        .published::<Order>(DEAD)
        .assert_called_once()
        .decoded_as::<Order>()
        .with(&Order { id: 6 });

    tb.shutdown().await.expect("shutdown");
}
