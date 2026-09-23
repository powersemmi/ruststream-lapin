//! Testing the production app: the builder `main` runs, handed to `TestApp` unchanged, with no
//! `RabbitMQ` server.
//!
//! The `testing` feature gives `LapinBroker` its in-process mode, which `TestApp::start` connects
//! instead of the server, and the test addresses the broker by its own type. A publish through the
//! harness returns once the handlers have settled, so the assertions after it never race them.
//!
//! ```text
//! cargo run --example lapin_testing --features testing
//! ```

use ruststream::OutSlot;
use ruststream::testing::TestApp;
use ruststream_lapin::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq, Outgoing)]
struct Payment {
    amount: u64,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Outgoing)]
struct Receipt {
    amount: u64,
}

#[derive(OutSlot)]
#[publishes(Receipt)]
struct Receipts;

// --8<-- [start:handler]
#[subscriber(RabbitQueue::new("payments"))]
async fn accept(payment: &Payment) -> HandlerOutcome {
    if payment.amount == 0 {
        return HandlerOutcome::drop();
    }
    HandlerOutcome::ack()
}
// --8<-- [end:handler]

/// A large payment is receipted ahead of the rest: the step names the priority for that one
/// message, and every other receipt takes the mount site's.
#[subscriber(RabbitQueue::new("payments.receipted"))]
async fn receipt_for(
    payment: &Payment,
    Out(receipts): Out<impl Publisher<Options = LapinPublishOptions>, Receipts>,
) -> HandlerOutcome {
    let receipt = Receipt {
        amount: payment.amount,
    };
    let sent = if payment.amount > 1_000 {
        receipts
            .message(&receipt)
            .to("receipts")
            .priority(9)
            .publish()
            .await
    } else {
        receipts.message(&receipt).to("receipts").publish().await
    };
    if sent.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

// --8<-- [start:app]
/// The service's app, the one `main` runs against the broker's address.
fn app() -> RustStream {
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(
        LapinBroker::new("amqp://localhost:5672"),
        |b| {
            b.include(accept);
            b.include(receipt_for)
                .out(Receipts, Publish::default().priority(3))
                .build();
        },
    )
}
// --8<-- [end:app]

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    // --8<-- [start:testapp]
    let tb = TestApp::start(app()).await.expect("start");

    tb.broker::<LapinBroker>()
        .message(&Payment { amount: 100 })
        .to("payments")
        .publish()
        .await
        .expect("publish drives the handler to quiescence");

    tb.broker::<LapinBroker>()
        .subscriber("payments")
        .assert_called_once()
        .with(&Payment { amount: 100 })
        .settled(HandlerOutcome::ack());
    // --8<-- [end:testapp]

    tb.broker::<LapinBroker>()
        .message(&Payment { amount: 5_000 })
        .to("payments.receipted")
        .publish()
        .await
        .expect("publish drives the handler to quiescence");

    // --8<-- [start:options]
    tb.out::<Receipts>()
        .assert_called_once()
        .with_options(&LapinPublishOptions {
            priority: Some(9),
            ..LapinPublishOptions::default()
        });
    // --8<-- [end:options]

    // --8<-- [start:published]
    tb.broker::<LapinBroker>()
        .published::<Receipt>("receipts")
        .assert_called_once()
        .with(&Receipt { amount: 5_000 });
    // --8<-- [end:published]

    tb.shutdown().await.expect("shutdown");

    println!("all in-process checks passed");
}
