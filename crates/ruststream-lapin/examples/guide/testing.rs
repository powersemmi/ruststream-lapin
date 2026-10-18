//! The service the Testing guide tests: card payments with a receipt for each, a hold for the
//! large ones and a review queue for the payments the bank keeps holding.
//!
//! ```text
//! cargo test --example guide_testing --features testing
//! AMQP_TEST_URL=amqp://127.0.0.1:5672 \
//!   cargo test --example guide_testing --features testing -- --test-threads=1
//! ```

use std::time::Duration;

use ruststream_lapin::prelude::*;
use serde::{Deserialize, Serialize};

#[cfg(test)]
#[path = "../../tests/live/mod.rs"]
mod live;

/// The largest payment the bank settles at once; a larger one is held.
const INSTANT_LIMIT_CENTS: u64 = 100_000;
/// How long a held payment waits before the service asks the bank again.
const HOLD: Duration = Duration::from_secs(60);

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "card-payments")]
struct Payment {
    id: u64,
    amount_cents: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "card-receipts")]
struct Receipt {
    payment: u64,
    amount_cents: u64,
}

/// Charges a payment and replies with its receipt.
#[subscriber(RabbitQueue::new("card-payments"), reply)]
async fn charge(payment: &Payment) -> Result<Receipt, HandlerOutcome> {
    if payment.amount_cents == 0 {
        tracing::warn!(payment.id, "payment without an amount dropped");
        return Err(HandlerOutcome::drop());
    }
    if payment.amount_cents > INSTANT_LIMIT_CENTS {
        tracing::info!(payment.id, "payment held by the bank");
        return Err(HandlerOutcome::retry_after(HOLD));
    }
    tracing::info!(payment.id, "payment charged");
    Ok(Receipt {
        payment: payment.id,
        amount_cents: payment.amount_cents,
    })
}

fn service(broker: LapinBroker) -> RustStream {
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(broker, |b| {
        b.include(charge)
            .max_attempts(nonzero!(3u32))
            .dead_letter("card-payments.review")
            .out_reply(Publish::default());
    })
}

#[ruststream::app]
fn app() -> impl App {
    service(LapinBroker::new("amqp://localhost:5672").declare_topology(true))
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::runtime::RETRY_COUNT_HEADER;
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
    async fn a_payment_the_bank_keeps_holding_goes_to_review() -> Result<(), Box<dyn Error>> {
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

        // The service holds the copy and publishes it back to the queue after `HOLD`.
        tb.advance(HOLD).await?;
        tb.broker::<LapinBroker>()
            .published::<Payment>("card-payments")
            .assert_called(2)
            .with(&held)
            .with_header(RETRY_COUNT_HEADER, "1");

        // The third delivery is the last one: it goes to the review queue as it arrived.
        tb.advance(HOLD).await?;
        assert_eq!(
            tb.broker::<LapinBroker>()
                .subscriber("card-payments")
                .outcomes(),
            [Outcome::Nack, Outcome::Nack, Outcome::Nack],
        );
        tb.broker::<LapinBroker>()
            .published::<Payment>("card-payments.review")
            .assert_called_once()
            .with(&held);
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
