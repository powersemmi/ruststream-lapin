//! In-process unit-testing: the same handlers and descriptors, no `RabbitMQ` server.
//!
//! The `testing` feature ships `LapinTestBroker`, an in-process stand-in for `RabbitMQ`. Build
//! the app around it exactly as in production and hand it to `TestApp`. A publish through the
//! harness returns once the handlers have settled, so the assertions after it never race them.
//!
//! ```text
//! cargo run --example lapin_testing --features testing
//! ```

use ruststream::testing::TestApp;
use ruststream_lapin::prelude::*;
use ruststream_lapin::testing::LapinTestBroker;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct Payment {
    amount: u64,
}

// --8<-- [start:handler]
#[subscriber(RabbitQueue::new("payments"))]
async fn accept(payment: &Payment) -> HandlerOutcome {
    if payment.amount == 0 {
        return HandlerOutcome::drop();
    }
    HandlerOutcome::ack()
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
        .settled(HandlerOutcome::ack());

    tb.shutdown().await.expect("shutdown");
    // --8<-- [end:testapp]

    println!("all in-process checks passed");
}
