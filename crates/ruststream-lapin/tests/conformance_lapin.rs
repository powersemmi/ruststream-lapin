//! Conformance suites. Each check verifies a different contract surface: `run_suite` proves
//! queue-name routing on the production broker connected in process; `lifecycle` proves the ladder
//! (synchronous construction, consuming `connect`, subscribe through the crate's own descriptor,
//! publish, ack, consuming `shutdown`, and a pre-shutdown publisher erroring afterwards) through
//! the real `LapinBroker`; `redelivery_address` holds both subscription forms to the address they
//! report, since a queue addresses its own redeliveries; the capability suites prove the optional
//! trait implementations, both transaction kinds and client-side batching included.
//!
//! The suites run twice, once over each transport of the same production broker. The in-process
//! pass, through `harness::InProcessBroker`, is what keeps the in-process mode honest: it runs the
//! very suites the live broker runs, over the same descriptors and policies. The live pass is the one that
//! proves the AMQP implementation, and it is gated behind `AMQP_TEST_URL` (see
//! `docker-compose.test.yml` and `just test-brokers`).

#![cfg(feature = "testing")]

use ruststream::Name;
use ruststream::conformance::harness::InProcessBroker;
use ruststream::conformance::{capabilities, harness};
use ruststream_lapin::{LapinBroker, LapinPublish, LapinRequest, RabbitQueue};

mod live;

/// The broker address, or `None` to skip. Under `RUSTSTREAM_REQUIRE_LIVE` a missing address
/// fails the suite instead of skipping it.
fn amqp_url() -> Option<String> {
    live::url("AMQP_TEST_URL")
}

/// The address the service's broker is built with; the in-process passes dial nothing.
const URI: &str = "amqp://localhost:5672";

/// The production broker, connected in process by the suites that take any broker.
fn in_process() -> InProcessBroker<LapinBroker> {
    InProcessBroker::new(LapinBroker::new(URI))
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
async fn the_in_process_mode_passes_conformance_suite() {
    harness::run_suite(|| LapinBroker::new(URI)).await;
}

// The ladder in process, the redelivery address included: the in-process mode answers with the
// queue name exactly as the live one does, so a suite run without a server already catches an
// answer that reaches nothing.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_passes_lifecycle() {
    harness::lifecycle(
        in_process,
        |name| RabbitQueue::new(name),
        |connected| connected.publisher(LapinPublish::default()),
    )
    .await;
}

// The same ladder over the bare-string form, which resolves through `Subscribe` rather than
// through the crate's descriptor.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_passes_lifecycle_by_name() {
    harness::lifecycle(
        in_process,
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

// A publish to the address the descriptor reports must reach the subscription that reported it:
// that address is where the runtime's deferred retry copy goes, so an answer that routes nowhere
// would lose every delayed redelivery.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_reports_a_redelivery_address_that_arrives() {
    harness::redelivery_address(
        in_process,
        |name| RabbitQueue::new(name),
        |connected| connected.publisher(LapinPublish::default()),
    )
    .await;
}

// The bare-name form answers through the broker rather than through the descriptor, so it is a
// second answer to hold to the same promise.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_reports_a_redelivery_address_that_arrives_by_name() {
    harness::redelivery_address(
        in_process,
        |name| Name::new(name.to_owned()),
        |connected| connected.publisher(LapinPublish::default()),
    )
    .await;
}

// The capability suites again, in process. Each production policy pairs into the publisher it
// produces on a server, over the in-process transport, and owes the same contract: these run the
// very suites the live broker runs below, against the same policy values a routes file writes.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_passes_request_reply() {
    capabilities::request_reply(
        in_process,
        |name| RabbitQueue::new(name),
        |connected| connected.requester(LapinRequest::default()),
        |connected| connected.publisher(LapinPublish::default()),
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_passes_transactions_with_confirms() {
    capabilities::transactions(
        in_process,
        |name| RabbitQueue::new(name),
        |connected| connected.publisher(LapinPublish::default().confirms()),
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_passes_owned_transactions_with_confirms() {
    capabilities::owned_transactions(
        in_process,
        |name| RabbitQueue::new(name),
        |connected| connected.publisher(LapinPublish::default().confirms()),
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_passes_transactions_with_server_tx() {
    capabilities::transactions(
        in_process,
        |name| RabbitQueue::new(name),
        |connected| connected.publisher(LapinPublish::default().server_tx()),
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_passes_batches() {
    capabilities::batches(
        in_process,
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

// The address on a server, where the default exchange is the server's own.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reports_a_redelivery_address_that_arrives() {
    let Some(url) = amqp_url() else { return };
    harness::redelivery_address(
        || LapinBroker::new(url.clone()).declare_topology(true),
        conformance_queue,
        |connected| connected.publisher(LapinPublish::default()),
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reports_a_redelivery_address_that_arrives_by_name() {
    let Some(url) = amqp_url() else { return };
    harness::redelivery_address(
        || LapinBroker::new(url.clone()).declare_topology(true),
        |name| Name::new(name.to_owned()),
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
