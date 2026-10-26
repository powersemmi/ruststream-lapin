//! The service the Testing guide tests, written without the macros: the handler is a type with an
//! `impl Handle`, mounted with the `subscriber` constructor, and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_guide_testing --features testing
//! AMQP_TEST_URL=amqp://127.0.0.1:5672 \
//!   cargo test --example manual_guide_testing --features testing -- --test-threads=1
//! ```

use std::future::ready;
use std::process::ExitCode;
use std::time::Duration;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_lapin::prelude::*;
use serde::{Deserialize, Serialize};

#[cfg(test)]
#[path = "../../../tests/live/mod.rs"]
mod live;

/// The largest payment the bank settles at once; a larger one is held.
const INSTANT_LIMIT_CENTS: u64 = 100_000;
/// How long a held payment waits before the service asks the bank again.
const HOLD: Duration = Duration::from_secs(60);

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "card-payments")]
struct Payment {
    id: u64,
    amount_cents: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "card-receipts")]
struct Receipt {
    payment: u64,
    amount_cents: u64,
}

/// Charges a payment and replies with its receipt.
struct Charge;

impl Handle<Payment, Receipt> for Charge {
    fn handle(
        &self,
        payment: &Payment,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Receipt, HandlerOutcome>> {
        if payment.amount_cents == 0 {
            tracing::warn!(payment.id, "payment without an amount dropped");
            return ready(Err(HandlerOutcome::drop()));
        }
        if payment.amount_cents > INSTANT_LIMIT_CENTS {
            tracing::info!(payment.id, "payment held by the bank");
            return ready(Err(HandlerOutcome::retry_after(HOLD)));
        }
        tracing::info!(payment.id, "payment charged");
        ready(Ok(Receipt {
            payment: payment.id,
            amount_cents: payment.amount_cents,
        }))
    }
}

fn service(broker: LapinBroker) -> RustStream {
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(broker, |b| {
        b.include(
            subscriber(
                RabbitQueue::new("card-payments").delay(Delay::dlx_ttl()),
                Charge,
            )
            .reply()
            .build(),
        )
        .dead_letter("card-payments.review")
        .out_reply(Publish::default());
    })
}

fn app() -> RustStream {
    service(LapinBroker::new("amqp://localhost:5672").declare_topology(true))
}

fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::{Outcome, TestApp};
    use ruststream_lapin::prelude::*;

    use super::{HOLD, Payment, Receipt, live, service};

    // --8<-- [start:in_process]
    #[tokio::test]
    async fn a_payment_gets_a_receipt() -> Result<(), Box<dyn Error>> {
        let broker = LapinBroker::new("amqp://localhost:5672").declare_topology(true);
        let tb = TestApp::start(service(broker)).await?;
        a_receipt_follows_a_payment(tb).await
    }

    async fn a_receipt_follows_a_payment(tb: TestApp<()>) -> Result<(), Box<dyn Error>> {
        tb.broker::<LapinBroker>()
            .message(&Payment {
                id: 1,
                amount_cents: 2_500,
            })
            .publish()
            .await?;

        tb.broker::<LapinBroker>()
            .subscriber("card-payments")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.broker::<LapinBroker>()
            .published::<Receipt>("card-receipts")
            .assert_called_once()
            .with(&Receipt {
                payment: 1,
                amount_cents: 2_500,
            });
        tb.shutdown().await?;
        Ok(())
    }
    // --8<-- [end:in_process]

    // --8<-- [start:dropped]
    #[tokio::test]
    async fn a_payment_without_an_amount_is_dropped() -> Result<(), Box<dyn Error>> {
        let broker = LapinBroker::new("amqp://localhost:5672").declare_topology(true);
        let tb = TestApp::start(service(broker)).await?;

        tb.broker::<LapinBroker>()
            .message(&Payment {
                id: 2,
                amount_cents: 0,
            })
            .publish()
            .await?;

        tb.broker::<LapinBroker>()
            .subscriber("card-payments")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        tb.broker::<LapinBroker>()
            .published::<Receipt>("card-receipts")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }
    // --8<-- [end:dropped]

    // --8<-- [start:held]
    #[tokio::test(start_paused = true)]
    async fn a_held_payment_comes_back_after_the_hold() -> Result<(), Box<dyn Error>> {
        let broker = LapinBroker::new("amqp://localhost:5672").declare_topology(true);
        let tb = TestApp::start(service(broker)).await?;
        let held = Payment {
            id: 3,
            amount_cents: 250_000,
        };

        tb.broker::<LapinBroker>().message(&held).publish().await?;
        tb.broker::<LapinBroker>()
            .subscriber("card-payments")
            .assert_called_once()
            .settled(HandlerOutcome::retry_after(HOLD));

        // The waiting queue holds the copy and releases it to `card-payments` after `HOLD`.
        tb.advance(HOLD).await?;
        tb.broker::<LapinBroker>()
            .subscriber("card-payments")
            .assert_called(2)
            .with(&held);
        assert_eq!(
            tb.broker::<LapinBroker>()
                .subscriber("card-payments")
                .outcomes(),
            [Outcome::Nack, Outcome::Nack],
        );
        // The broker kept the copy: the one publish to `card-payments` is the test's own.
        tb.broker::<LapinBroker>()
            .published::<Payment>("card-payments")
            .assert_called_once();
        tb.shutdown().await?;
        Ok(())
    }
    // --8<-- [end:held]

    // --8<-- [start:live]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_payment_gets_a_receipt_on_a_server() -> Result<(), Box<dyn Error>> {
        let Some(url) = live::url("AMQP_TEST_URL") else {
            return Ok(());
        };
        let tb = TestApp::start_live(service(LapinBroker::new(url).declare_topology(true))).await?;
        a_receipt_follows_a_payment(tb).await
    }
    // --8<-- [end:live]
}
