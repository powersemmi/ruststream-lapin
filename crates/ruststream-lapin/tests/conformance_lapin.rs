//! Conformance: the in-process transport passes `run_suite` unconditionally; the lifecycle and
//! capability suites run against a real `RabbitMQ` when `AMQP_TEST_URL` is set (see
//! `docker-compose.test.yml` and `just test-brokers`).

#![cfg(feature = "testing")]

use ruststream::conformance::{capabilities, harness};
use ruststream_lapin::testing::LapinTestBroker;
use ruststream_lapin::{LapinBroker, RabbitQueue};

fn amqp_url() -> Option<String> {
    std::env::var("AMQP_TEST_URL").ok()
}

/// Conformance queues are throwaways; exclusive transient queues vanish with the connection
/// (`RabbitMQ` 4 denies transient non-exclusive queues by default).
fn conformance_queue(name: &str) -> RabbitQueue {
    RabbitQueue::new(name).durable(false).exclusive(true)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lapin_test_broker_passes_conformance_suite() {
    harness::run_suite(LapinTestBroker::new).await;
}

// The harness takes higher-ranked closures that method paths cannot satisfy.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn passes_lifecycle() {
    let Some(url) = amqp_url() else { return };
    harness::lifecycle(
        || LapinBroker::new(url.clone()).declare_topology(true),
        conformance_queue,
        |broker| broker.publisher(),
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
        |broker| broker.publisher().confirms(),
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
        |broker| broker.publisher().server_tx(),
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
        |broker| broker.requester(),
        |broker| broker.publisher(),
    )
    .await;
}
