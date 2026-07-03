//! In-process unit-testing: the same handlers and descriptors, no `RabbitMQ` server.
//!
//! The `testing` feature ships `LapinTestBroker`, an in-process transport that is a full broker
//! and also implements `ruststream::testing::TestableBroker`. Build the app around it exactly
//! as in production, drive publishes with `TestApp::publish` (which waits for quiescence), and
//! assert on what the handlers did.
//!
//! ```text
//! cargo run --example lapin_testing --features testing
//! ```

use ruststream::conformance::harness;
use ruststream::runtime::{AppInfo, HandlerResult, RustStream};
use ruststream::subscriber;
use ruststream::testing::TestApp;
use ruststream_lapin::RabbitQueue;
use ruststream_lapin::testing::LapinTestBroker;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct Payment {
    amount: u64,
}

// --8<-- [start:handler]
#[subscriber(RabbitQueue::new("payments"))]
async fn accept(payment: &Payment) -> HandlerResult {
    if payment.amount == 0 {
        return HandlerResult::drop();
    }
    HandlerResult::Ack
}
// --8<-- [end:handler]

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    // --8<-- [start:testapp]
    let app = RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(
        LapinTestBroker::new(),
        |b| {
            b.include(accept);
        },
    );
    let tb = TestApp::start(app).await.expect("start");

    tb.broker::<LapinTestBroker>()
        .publish("payments", &Payment { amount: 100 })
        .await
        .expect("publish drives the handler to quiescence");

    tb.broker::<LapinTestBroker>()
        .subscriber("payments")
        .assert_called_once()
        .with(&Payment { amount: 100 })
        .settled(HandlerResult::Ack);

    tb.shutdown().await.expect("shutdown");
    // --8<-- [end:testapp]

    // --8<-- [start:conformance]
    // The in-process transport itself is held to the core broker contract.
    harness::run_suite(LapinTestBroker::new).await;
    // --8<-- [end:conformance]

    println!("all in-process checks passed");
}
