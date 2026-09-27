//! Conformance suites. Each check verifies a different contract surface: `run_suite` proves
//! queue-name routing on the production broker connected in process; `lifecycle` proves the ladder
//! (synchronous construction, consuming `connect`, subscribe through the crate's own descriptor,
//! publish, ack, consuming `shutdown`, and a pre-shutdown publisher erroring afterwards) through
//! the real `LapinBroker`; `redelivery_address` holds both subscription forms to the address they
//! report, since a queue addresses its own redeliveries; `broker_moves` holds a quorum queue to the
//! delivery limit and dead-letter route it applies itself; the message-shape suites hold a key to
//! its order, the publish options to the policy they resolve over, and the generated document to
//! carrying no credentials; the capability suites prove the optional trait implementations, both
//! transaction kinds and client-side batching included.
//!
//! The suites run twice, once over each transport of the same production broker. The in-process
//! pass, through `harness::InProcessBroker`, is what keeps the in-process mode honest: it runs the
//! very suites the live broker runs, over the same descriptors and policies. The live pass is the
//! one that proves the AMQP implementation, and it is gated behind `AMQP_TEST_URL` (see
//! `docker-compose.test.yml` and `just test-brokers`). Two suites compare the transports directly
//! against the server: what a queue keeps for a subscription that opens later, and what each of
//! them refuses.

#![cfg(feature = "testing")]

use std::num::NonZeroU32;
use std::time::Duration;

use ruststream::conformance::harness::InProcessBroker;
use ruststream::conformance::helpers::unique_subject;
use ruststream::conformance::in_process::{self, Refusal};
use ruststream::conformance::message_shape::{self, OptionCases};
use ruststream::conformance::{capabilities, harness, retry};
use ruststream::{Bytes, HeaderMap, IncomingMessage, Name};
use ruststream_lapin::{
    ConnectedLapinBroker, EXPIRATION_HEADER, LapinBroker, LapinMessage, LapinPublish,
    LapinPublishOptions, LapinRequest, PARTITION_KEY_HEADER, PRIORITY_HEADER, RabbitQueue,
    RabbitQuorumQueue,
};

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

// ---------------------------------------------------------------------------------------------
// The contract past the ladder: settlement meanings, the retry path, what a message carries and
// what the in-process transport declares about the server.
// ---------------------------------------------------------------------------------------------

/// The production broker a deployment builds, dialling `url` and declaring its topology.
fn live_broker(url: &str) -> LapinBroker {
    LapinBroker::new(url).declare_topology(true)
}

/// A queue that outlives the connection that declared it, as a service's own queue does: the
/// checks that read it again from a second connection need it to be there.
fn lasting_queue(name: &str) -> RabbitQueue {
    RabbitQueue::new(name)
}

/// The priority and the TTL a delivery reports, which is how the publish options show on it.
type Shown = (Option<Vec<u8>>, Option<Vec<u8>>);

/// What `delivery` reports of the options it was published with.
fn observed_options(delivery: &LapinMessage) -> Shown {
    (
        delivery.headers().get(PRIORITY_HEADER).map(<[u8]>::to_vec),
        delivery
            .headers()
            .get(EXPIRATION_HEADER)
            .map(<[u8]>::to_vec),
    )
}

/// The cases the publish options check runs: a policy that fixes a priority and a TTL the
/// transport would not pick by itself, and calls that override one of them or both.
fn option_cases() -> OptionCases<LapinPublishOptions, Shown> {
    let shown = |priority: &[u8], ttl: &[u8]| (Some(priority.to_vec()), Some(ttl.to_vec()));
    OptionCases::new(shown(b"5", b"60000"))
        .overrides(
            LapinPublishOptions {
                priority: Some(1),
                ..LapinPublishOptions::default()
            },
            shown(b"1", b"60000"),
        )
        .overrides(
            LapinPublishOptions {
                priority: Some(9),
                expiration: Some(Duration::from_secs(30)),
                persistent: Some(false),
            },
            shown(b"9", b"30000"),
        )
}

/// The policy the publish options check pairs.
fn options_policy() -> LapinPublish {
    LapinPublish::default()
        .priority(5)
        .expiration(Duration::from_secs(60))
}

/// Carries a key the way this crate does: in the header its deliveries report it from.
#[allow(clippy::unnecessary_wraps)]
fn key_header(key: &[u8], headers: &mut HeaderMap) -> Option<LapinPublishOptions> {
    headers.insert(PARTITION_KEY_HEADER, Bytes::copy_from_slice(key));
    None
}

/// The attempts the quorum queue check declares.
const ATTEMPTS: NonZeroU32 = NonZeroU32::new(3).expect("three is not zero");

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_quorum_queue_moves_a_spent_delivery() {
    retry::broker_moves(
        || InProcessBroker::new(LapinBroker::new(URI).declare_topology(true)),
        |name| RabbitQuorumQueue::new(name),
        |connected| connected.publisher(LapinPublish::default()),
        ATTEMPTS,
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn quorum_queue_moves_a_spent_delivery() {
    let Some(url) = amqp_url() else { return };
    retry::broker_moves(
        || live_broker(&url),
        |name| RabbitQuorumQueue::new(name),
        |connected| connected.publisher(LapinPublish::default()),
        ATTEMPTS,
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_backlog_matches_the_server() {
    let Some(url) = amqp_url() else { return };
    in_process::backlog_matches_server(
        || live_broker(&url),
        |connected| connected.publisher(LapinPublish::default()),
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_refuses_like_the_server() {
    let Some(url) = amqp_url() else { return };
    let over_short = "q".repeat(256);
    let conflict = unique_subject("conformance.conflict");
    in_process::refuses_like_the_server(
        || live_broker(&url),
        |connected| connected.publisher(LapinPublish::default()),
        [
            // A routing key is a short string: 255 bytes at most.
            Refusal::Publish {
                name: over_short.clone(),
            },
            Refusal::Subscription {
                source: RabbitQueue::new(over_short),
            },
            // A queue declared again with other settings.
            Refusal::Conflicting {
                open: RabbitQueue::new(conflict.clone()),
                refused: RabbitQueue::new(conflict).auto_delete(true),
            },
        ],
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_keeps_one_key_in_order() {
    message_shape::keyed_order(
        in_process,
        &unique_subject("conformance.keyed"),
        lasting_queue,
        |connected| connected.publisher(LapinPublish::default()),
        key_header,
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn keeps_one_key_in_order() {
    let Some(url) = amqp_url() else { return };
    message_shape::keyed_order(
        || live_broker(&url),
        &unique_subject("conformance.keyed"),
        conformance_queue,
        |connected| connected.publisher(LapinPublish::default()),
        key_header,
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_publish_options_resolve_over_the_policy() {
    message_shape::publish_options(
        in_process,
        &unique_subject("conformance.options"),
        lasting_queue,
        options_policy(),
        option_cases(),
        observed_options,
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publish_options_resolve_over_the_policy() {
    let Some(url) = amqp_url() else { return };
    message_shape::publish_options(
        || live_broker(&url),
        &unique_subject("conformance.options"),
        conformance_queue,
        options_policy(),
        option_cases(),
        observed_options,
    )
    .await;
}

#[test]
fn publish_policies_describe_no_credentials() {
    for policy in [
        LapinPublish::default(),
        LapinPublish::default().exchange("events"),
    ] {
        message_shape::publishes_without_credentials::<ConnectedLapinBroker, _>(&policy, "hunter2");
    }
}

#[test]
fn the_server_address_carries_no_credentials() {
    // The broker dials one AMQP URI: it is built from the first address it is handed.
    message_shape::describes_addresses_without_credentials(
        |addrs| LapinBroker::new(addrs[0]),
        "amqp",
    );
}
