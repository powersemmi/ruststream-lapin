//! Testing a service in process: the production app, `LapinBroker` included, run by `TestApp`
//! with no `RabbitMQ` server. A publish through the harness returns once every handler it woke has
//! settled, so the assertions read finished state.
//!
//! ```text
//! cargo run --example lapin_testing_in_process --features testing -- run
//! cargo test --example lapin_testing_in_process --features testing
//! ```

use serde::{Deserialize, Serialize};

use ruststream_lapin::prelude::*;

#[derive(Debug, Serialize, Deserialize, PartialEq, Outgoing)]
#[outgoing(name = "payments")]
struct Payment {
    amount: u64,
}

/// A receipt leaves through the `receipts` exchange under the routing key `receipt.issued`.
#[derive(Debug, Serialize, Deserialize, PartialEq, Outgoing)]
#[outgoing(name = "receipt.issued")]
struct Receipt {
    amount: u64,
}

#[derive(OutSlot)]
#[publishes(Receipt)]
struct Receipts;

/// Accepts a payment and receipts it; a large payment's receipt goes out ahead of the rest.
#[subscriber(RabbitQueue::new("payments"))]
async fn accept(
    payment: &Payment,
    Out(receipts): Out<impl Publisher<Options = LapinPublishOptions>, Receipts>,
) -> HandlerOutcome {
    if payment.amount == 0 {
        return HandlerOutcome::drop();
    }
    let receipt = Receipt {
        amount: payment.amount,
    };
    let sent = if payment.amount > 1_000 {
        receipts.message(&receipt).priority(9).publish().await
    } else {
        receipts.message(&receipt).publish().await
    };
    if sent.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

/// Audits every receipt the topic exchange routes to its queue.
#[subscriber(
    RabbitQueue::new("receipts.audit").bind(RabbitExchange::topic("receipts"), "receipt.*")
)]
async fn audit(receipt: &Receipt) -> HandlerOutcome {
    tracing::info!(amount = receipt.amount, "receipt audited");
    HandlerOutcome::ack()
}

/// The app `main` runs in production, and the one a test hands the harness unchanged.
#[ruststream::app]
fn app() -> RustStream {
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(
        LapinBroker::new("amqp://localhost:5672"),
        |b| {
            b.include(accept)
                .out(
                    Receipts,
                    Publish::default().exchange("receipts").priority(3),
                )
                .build();
            b.include(audit);
        },
    )
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_lapin::prelude::*;

    use super::{Payment, Receipt, Receipts, app};

    // --8<-- [start:testapp]
    #[tokio::test]
    async fn a_payment_is_accepted() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<LapinBroker>()
            .message(&Payment { amount: 100 })
            .publish()
            .await?;

        tb.broker::<LapinBroker>()
            .subscriber("payments")
            .assert_called_once()
            .with(&Payment { amount: 100 })
            .settled(HandlerOutcome::ack());
        Ok(())
    }
    // --8<-- [end:testapp]

    // --8<-- [start:exchanges]
    #[tokio::test]
    async fn a_receipt_reaches_the_queue_bound_to_its_exchange() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<LapinBroker>()
            .message(&Payment { amount: 100 })
            .publish()
            .await?;

        // `receipts` is a topic exchange, and `receipts.audit` is bound to it under `receipt.*`.
        tb.broker::<LapinBroker>()
            .subscriber("receipts.audit")
            .assert_called_once()
            .with(&Receipt { amount: 100 })
            .settled(HandlerOutcome::ack());
        Ok(())
    }
    // --8<-- [end:exchanges]

    // --8<-- [start:options]
    #[tokio::test]
    async fn a_large_payment_is_receipted_first() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<LapinBroker>()
            .message(&Payment { amount: 5_000 })
            .publish()
            .await?;

        // The step's priority, over the policy's default of 3.
        tb.out::<Receipts>()
            .assert_called_once()
            .with_options(&LapinPublishOptions {
                priority: Some(9),
                ..LapinPublishOptions::default()
            });
        Ok(())
    }
    // --8<-- [end:options]
}
