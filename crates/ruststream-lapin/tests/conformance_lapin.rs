//! Conformance suites. Each check verifies a different contract surface: `run_suite` proves
//! queue-name routing in process against the `LapinTestBroker`'s
//! [`TestableBroker`](ruststream::testing::TestableBroker) impl; `lifecycle` proves the ladder
//! (synchronous construction, consuming `connect`, subscribe through the crate's own descriptor,
//! publish, ack, consuming `shutdown`, and a pre-shutdown publisher erroring afterwards) through
//! the real `LapinBroker`; the capability suites prove the optional trait implementations, both
//! transaction kinds and client-side batching included.
//!
//! The capability suites run twice, once against each transport. The in-process pass is what
//! keeps the test broker honest: it claims a capability only where the real publisher has one,
//! and the suite it passes is the suite the real publisher passes. The live pass is the one that
//! proves the AMQP implementation, and it is gated behind `AMQP_TEST_URL` (see
//! `docker-compose.test.yml` and `just test-brokers`).

#![cfg(feature = "testing")]

use ruststream::Name;
use ruststream::conformance::{capabilities, harness};
use ruststream_lapin::testing::LapinTestBroker;
use ruststream_lapin::{LapinBroker, LapinPublish, LapinRequest, RabbitQueue};

mod live;

/// The broker address, or `None` to skip. Under `RUSTSTREAM_REQUIRE_LIVE` a missing address
/// fails the suite instead of skipping it.
fn amqp_url() -> Option<String> {
    live::url("AMQP_TEST_URL")
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

// The ladder in process, the redelivery address included: the test broker answers with the queue
// name exactly as the live one does, so a suite run without a server already catches an answer
// that reaches nothing.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_broker_passes_lifecycle() {
    harness::lifecycle(
        LapinTestBroker::new,
        |name| RabbitQueue::new(name),
        |connected| connected.publisher(LapinPublish::default()),
    )
    .await;
}

// The same ladder over the bare-string form, which resolves through `Subscribe` rather than
// through the crate's descriptor: `#[subscriber("orders")]` has a redelivery address of its own to
// answer, and it is answered by a different method.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_broker_passes_lifecycle_by_name() {
    harness::lifecycle(
        LapinTestBroker::new,
        |name| Name::new(name.to_owned()),
        |connected| connected.publisher(LapinPublish::default()),
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn passes_lifecycle_by_name() {
    let Some(url) = amqp_url() else { return };
    harness::lifecycle(
        || LapinBroker::new(url.clone()).declare_topology(true),
        |name| Name::new(name.to_owned()),
        |connected| connected.publisher(LapinPublish::default()),
    )
    .await;
}

// The capability suites again, in process. Each production policy pairs against the test broker
// into the stand-in for the publisher it produces on a server, and a stand-in that claims a
// capability owes the same contract: these run the very suites the live broker runs below,
// against the same policy values a routes file writes.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_broker_passes_request_reply() {
    capabilities::request_reply(
        LapinTestBroker::new,
        |name| RabbitQueue::new(name),
        |connected| connected.requester(LapinRequest::default()),
        |connected| connected.publisher(LapinPublish::default()),
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_broker_passes_transactions_with_confirms() {
    capabilities::transactions(
        LapinTestBroker::new,
        |name| RabbitQueue::new(name),
        |connected| connected.publisher(LapinPublish::default().confirms()),
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_broker_passes_owned_transactions_with_confirms() {
    capabilities::owned_transactions(
        LapinTestBroker::new,
        |name| RabbitQueue::new(name),
        |connected| connected.publisher(LapinPublish::default().confirms()),
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_broker_passes_transactions_with_server_tx() {
    capabilities::transactions(
        LapinTestBroker::new,
        |name| RabbitQueue::new(name),
        |connected| connected.publisher(LapinPublish::default().server_tx()),
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_broker_passes_batches() {
    capabilities::batches(
        LapinTestBroker::new,
        |name| RabbitQueue::new(name),
        |connected| connected.publisher(LapinPublish::default()),
    )
    .await;
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

// AMQP has no wire batch, so the batches come from the crate's client-side buffer; the suite
// proves that buffer honours the size a registration opens the subscription with.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn passes_batches() {
    let Some(url) = amqp_url() else { return };
    capabilities::batches(
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

// The owned kind, which only the confirms publisher offers: its transaction is a client-side
// buffer, while `server_tx` puts the channel itself into transactional mode.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn passes_owned_transactions_with_confirms() {
    let Some(url) = amqp_url() else { return };
    capabilities::owned_transactions(
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
