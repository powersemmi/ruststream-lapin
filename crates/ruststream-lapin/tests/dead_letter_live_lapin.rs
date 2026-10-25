//! Dead-lettering on a classic queue against a real `RabbitMQ`.
//!
//! The server moves a rejected delivery to the queue's dead-letter route, so how many copies reach
//! the dead-letter queue is the server's answer and cannot be proved in process. Every test is a
//! no-op unless `AMQP_TEST_URL` points at a broker (see `just test-brokers`).

use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use lapin::options::{QueueDeclareOptions, QueueDeleteOptions};
use lapin::{Connection, ConnectionProperties};
use ruststream::testing::TestApp;
use ruststream::{ConnectedBroker, FromRef, OutgoingMessage};
use ruststream_lapin::prelude::*;
use serde::Deserialize;
use tokio::task::yield_now;
use tokio::time::{sleep, timeout as timeout_future};

use crate::live;

const WAIT: Duration = Duration::from_secs(5);

/// How long the dead-letter queue has to stay at one message for a second copy to count as absent.
const SILENCE: Duration = Duration::from_millis(500);

fn amqp_url() -> Option<String> {
    live::url("AMQP_TEST_URL")
}

/// The queue the service consumes and the queue a rejected delivery goes to. The attribute takes a
/// literal, so these are fixed names the test cleans up after itself.
const DROPPED: &str = "ruststream-dead-letter.classic";
const DROPPED_DEAD: &str = "ruststream-dead-letter.classic.dead";

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

/// How many deliveries the handler has seen.
#[derive(Clone, Default, FromRef)]
struct Seen {
    deliveries: Arc<AtomicU32>,
}

/// Asks for the first delivery back, and drops the second.
#[subscriber(RabbitQueue::new(DROPPED)
    .dead_letter_exchange("")
    .dead_letter_routing_key(DROPPED_DEAD))]
async fn retry_then_drop(order: &Order, State(seen): State<Arc<AtomicU32>>) -> HandlerOutcome {
    let _ = order.id;
    if seen.fetch_add(1, Ordering::Relaxed) == 0 {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::drop()
}

// A classic queue with a dead-letter route of its own, under a registration that declares the
// same destination: a retry goes back to the queue, and the dropped delivery reaches the
// dead-letter queue once, moved by the server, with nothing published by the service beside it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dropped_delivery_reaches_the_dead_letter_queue_once() {
    let Some(url) = amqp_url() else { return };
    delete_queues(&url, &[DROPPED, DROPPED_DEAD]).await;
    declare_queue(&url, DROPPED_DEAD).await;

    let seen = Seen::default();
    let deliveries = Arc::clone(&seen.deliveries);
    let probe = url.clone();
    let app = RustStream::new(AppInfo::new("dropped", "0.1.0"))
        .on_startup(move |()| {
            let seen = seen;
            async move { Ok::<_, Infallible>(seen) }
        })
        .with_broker(LapinBroker::new(url.clone()).declare_topology(true), |b| {
            b.include(retry_then_drop).dead_letter(DROPPED_DEAD);
        });
    let running = tokio::spawn(app.run_until(async move {
        settled_count(&probe, DROPPED_DEAD).await;
    }));

    let publisher = LapinBroker::new(url.clone())
        .connect()
        .await
        .expect("connect");
    wait_for_consumer(&url, DROPPED).await;
    publisher
        .publisher(LapinPublish::default())
        .publish(OutgoingMessage::new(DROPPED, br#"{"id":7}"#), None)
        .await
        .expect("publish");

    timeout_future(WAIT, running)
        .await
        .expect("the message reaches the dead-letter queue")
        .expect("app task did not panic")
        .expect("run_until succeeded");

    assert_eq!(
        ready_messages(&url, DROPPED_DEAD).await,
        1,
        "one dropped message is one dead letter",
    );
    assert_eq!(
        deliveries.load(Ordering::Relaxed),
        2,
        "the retry went back to the queue, and the second delivery was dropped",
    );

    publisher.shutdown().await.expect("shutdown");
    delete_queues(&url, &[DROPPED, DROPPED_DEAD]).await;
}

/// A queue that already exists with a dead-letter route of its own, and the destination the
/// mount site declares for it.
const MISMATCHED: &str = "ruststream-dead-letter.mismatched";
const MISMATCHED_DEAD: &str = "ruststream-dead-letter.mismatched.dead";

#[subscriber(RabbitQueue::new(MISMATCHED))]
async fn set_aside(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::drop()
}

// The declared route has to be the queue's own: a service that declares its topology redeclares
// the queue with the route, the server refuses arguments other than the ones the queue carries,
// and the registration does not start, with the queue and the arguments in the refusal.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_route_the_queue_does_not_carry_refuses_to_start() {
    let Some(url) = amqp_url() else { return };
    delete_queues(&url, &[MISMATCHED]).await;
    declare_queue(&url, MISMATCHED).await;

    let app = RustStream::new(AppInfo::new("mismatched", "0.1.0")).with_broker(
        LapinBroker::new(url.clone()).declare_topology(true),
        |b| {
            b.include(set_aside).dead_letter(MISMATCHED_DEAD);
        },
    );
    let err = TestApp::start_live(app)
        .await
        .expect_err("the server refuses a route the queue does not carry");
    let message = err.to_string();
    assert!(message.contains(MISMATCHED), "{message}");
    assert!(message.contains("x-dead-letter-routing-key"), "{message}");
    assert!(message.contains(MISMATCHED_DEAD), "{message}");

    delete_queues(&url, &[MISMATCHED]).await;
}

/// Returns once `queue` holds a message and has kept the same count for [`SILENCE`], so a second
/// copy on its way has had the time to arrive.
async fn settled_count(url: &str, queue: &str) {
    loop {
        let seen = ready_messages(url, queue).await;
        if seen > 0 {
            sleep(SILENCE).await;
            if ready_messages(url, queue).await == seen {
                return;
            }
        }
        yield_now().await;
    }
}

async fn declare_queue(url: &str, queue: &str) {
    let connection = Connection::connect(url, ConnectionProperties::default())
        .await
        .expect("declare connect");
    let channel = connection.create_channel().await.expect("declare channel");
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
        .expect("declare the dead-letter queue");
    connection
        .close(200, "OK".into())
        .await
        .expect("declare close");
}

/// How many messages `queue` holds, as the server counts them.
async fn ready_messages(url: &str, queue: &str) -> u32 {
    let probe = Connection::connect(url, ConnectionProperties::default())
        .await
        .expect("probe connect");
    let channel = probe.create_channel().await.expect("probe channel");
    let state = channel
        .queue_declare(
            queue.into(),
            QueueDeclareOptions {
                passive: true,
                durable: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .expect("passive declare");
    let ready = state.message_count();
    probe.close(200, "OK".into()).await.expect("probe close");
    ready
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

async fn wait_for_consumer(url: &str, queue: &str) {
    let connection = Connection::connect(url, ConnectionProperties::default())
        .await
        .expect("readiness connection");
    timeout_future(WAIT, async {
        loop {
            let channel = connection
                .create_channel()
                .await
                .expect("readiness channel");
            let declared = channel
                .queue_declare(
                    queue.into(),
                    QueueDeclareOptions {
                        passive: true,
                        ..QueueDeclareOptions::default()
                    },
                    FieldTable::default(),
                )
                .await;
            if declared.is_ok_and(|queue| queue.consumer_count() > 0) {
                break;
            }
            yield_now().await;
        }
    })
    .await
    .expect("the service consumer starts");
    drop(connection);
}
