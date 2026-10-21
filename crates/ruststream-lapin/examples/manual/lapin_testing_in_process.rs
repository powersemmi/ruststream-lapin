//! The service and tests of `examples/lapin_testing_in_process.rs` without the macros: the
//! handlers are named types mounted on their queue descriptors, and `main` is written by hand.
//! The tests are the same: the harness drives the app whatever form it is written in.
//!
//! ```text
//! cargo run --example manual_lapin_testing_in_process --features testing -- run
//! cargo test --example manual_lapin_testing_in_process --features testing
//! ```

use std::future::ready;
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use ruststream_lapin::prelude::*;

#[derive(Debug, Serialize, Deserialize, PartialEq, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Payment {
    amount: u64,
}

// `#[outgoing(name = "payments")]` by hand: the type names the queue it is published to.
impl OutgoingDestination for Payment {
    type Form = FixedName;
    const DESTINATION: &'static str = "payments";
}

impl MessageHeaders for Payment {
    type Contract = NoHeaders;
}

#[derive(Debug, Serialize, Deserialize, PartialEq, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Receipt {
    amount: u64,
}

// `#[outgoing(name = "receipt.issued")]` by hand: the routing key a receipt leaves under.
impl OutgoingDestination for Receipt {
    type Form = FixedName;
    const DESTINATION: &'static str = "receipt.issued";
}

impl MessageHeaders for Receipt {
    type Contract = NoHeaders;
}

/// The slot receipts leave through.
struct Receipts;

impl OutSlot for Receipts {
    const NAME: &'static str = "Receipts";
    // `Receipt` fixes its destination, so no transform on this slot may name one.
    type Destination = Reads;
}

impl PublishedThrough<Receipts> for Receipt {}

/// Accepts a payment and receipts it; a large payment's receipt goes out ahead of the rest.
struct Accept;

impl<Entry> Handle<Payment, (), Outs<(Entry,)>> for Accept
where
    Entry: OutEntry<Receipts, Wire: Publisher<Options = LapinPublishOptions>>,
{
    async fn handle(
        &self,
        payment: &Payment,
        outs: &Outs<(Entry,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        if payment.amount == 0 {
            return Err(HandlerOutcome::drop());
        }
        let receipts = outs.get(Receipts);
        let receipt = Receipt {
            amount: payment.amount,
        };
        let sent = if payment.amount > 1_000 {
            receipts.message(&receipt).priority(9).publish().await
        } else {
            receipts.message(&receipt).publish().await
        };
        if sent.is_err() {
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
}

/// Audits every receipt the topic exchange routes to its queue.
struct Audit;

impl Handle<Receipt> for Audit {
    fn handle(
        &self,
        receipt: &Receipt,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(amount = receipt.amount, "receipt audited");
        ready(Ok(()))
    }
}

/// The app `main` runs in production, and the one a test hands the harness unchanged.
fn app() -> RustStream {
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(
        LapinBroker::new("amqp://localhost:5672"),
        |b| {
            b.include(subscriber(RabbitQueue::new("payments"), Accept).build())
                .out(
                    Receipts,
                    Publish::default().exchange("receipts").priority(3),
                )
                .build();
            let audited = RabbitQueue::new("receipts.audit")
                .bind(RabbitExchange::topic("receipts"), "receipt.*");
            b.include(subscriber(audited, Audit).build());
        },
    )
}

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
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
