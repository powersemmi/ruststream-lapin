//! Conformance suites. Each check verifies a different contract surface: `run_suite` proves
//! queue-name routing in process against the `LapinTestBroker`'s
//! [`TestableBroker`](ruststream::testing::TestableBroker) impl; `lifecycle` proves the ladder
//! (synchronous construction, consuming `connect`, subscribe through the crate's own descriptor,
//! publish, ack, consuming `shutdown`, and a pre-shutdown publisher erroring afterwards) through
//! the real `LapinBroker`; the capability suites prove the optional trait implementations. All
//! but `run_suite` are gated behind `AMQP_TEST_URL` (see `docker-compose.test.yml` and
//! `just test-brokers`).

#![cfg(feature = "testing")]

use ruststream::conformance::{capabilities, harness};
use ruststream_lapin::testing::LapinTestBroker;
use ruststream_lapin::{LapinBroker, LapinPublish, LapinRequest, RabbitQueue};

fn amqp_url() -> Option<String> {
    std::env::var("AMQP_TEST_URL").ok()
}

/// Conformance queues are throwaways: auto-deleted once the suite's consumer goes away.
///
/// Deliberately not exclusive. Two suites share a subject name (both transaction suites run on
/// `conformance.transactions`), and an exclusive queue stays locked to its connection until the
/// server has finished tearing that connection down, so the next suite would race a
/// `RESOURCE_LOCKED` declare. They stay durable because `RabbitMQ` 4 denies transient
/// non-exclusive queues by default.
fn conformance_queue(name: &str) -> RabbitQueue {
    RabbitQueue::new(name).auto_delete(true)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lapin_test_broker_passes_conformance_suite() {
    harness::run_suite(LapinTestBroker::new).await;
}

// `make_source` / `make_publisher` must stay closures: their bounds are higher-ranked
// (`Fn(&str) -> _` / `Fn(&C) -> _`), so a bare method path - which binds one concrete lifetime -
// would not type-check.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn passes_lifecycle() {
    let Some(url) = amqp_url() else { return };
    harness::lifecycle(
        || LapinBroker::new(url.clone()).declare_topology(true),
        conformance_queue,
        |connected| connected.publisher(LapinPublish::default()),
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn passes_transactions_with_confirms() {
    let Some(url) = amqp_url() else { return };
    capabilities::transactions(
        || LapinBroker::new(url.clone()).declare_topology(true),
        conformance_queue,
        |connected| connected.publisher(LapinPublish::default().confirms()),
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn passes_transactions_with_server_tx() {
    let Some(url) = amqp_url() else { return };
    capabilities::transactions(
        || LapinBroker::new(url.clone()).declare_topology(true),
        conformance_queue,
        |connected| connected.publisher(LapinPublish::default().server_tx()),
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn passes_request_reply() {
    let Some(url) = amqp_url() else { return };
    capabilities::request_reply(
        || LapinBroker::new(url.clone()).declare_topology(true),
        conformance_queue,
        |connected| connected.requester(LapinRequest::default()),
        |connected| connected.publisher(LapinPublish::default()),
    )
    .await;
}
