//! A retry cap on a classic queue: the runtime counts the deliveries in a header of the copies it
//! publishes back to the queue, and sends the last one to a dead-letter queue.
//!
//! ```text
//! cargo test --example lapin_settlement_retries --features testing
//! cargo run --example lapin_settlement_retries -- run
//! ```

use ruststream::nonzero;
use ruststream::runtime::RETRY_COUNT_HEADER;
use ruststream_lapin::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "payments")]
struct Payment {
    id: u64,
    card: String,
}

// --8<-- [start:retries]
/// A card the gateway declines is asked for again, at once.
#[subscriber(RabbitQueue::new("payments"))]
async fn charge(payment: &Payment) -> HandlerOutcome {
    if payment.card.starts_with("0000") {
        tracing::warn!(payment.id, "card declined, retrying");
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

/// The payments that ran out of deliveries, with the count the runtime kept.
#[subscriber("payments.dead")]
async fn bury(payment: &Payment, ctx: &mut Context<'_>) -> HandlerOutcome {
    let deliveries = ctx.headers().get_str(RETRY_COUNT_HEADER).unwrap_or("1");
    tracing::error!(payment.id, deliveries, "payment dead-lettered");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    let broker = LapinBroker::new("amqp://localhost:5672");
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(broker, |b| {
        // Three deliveries, the first included, then the payment goes to `payments.dead`. The
        // copies leave through the confirms policy, which waits for the broker to confirm each.
        b.include(charge)
            .max_attempts(nonzero!(3u32))
            .dead_letter("payments.dead")
            .out_retry(TransactionalPublish::default());
        b.include(bury);
    })
}
// --8<-- [end:retries]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::runtime::RETRY_COUNT_HEADER;
    use ruststream::testing::TestApp;
    use ruststream_lapin::prelude::*;

    use super::{Payment, app};

    fn declined() -> Payment {
        Payment {
            id: 9,
            card: "0000-1111".into(),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_third_failure_goes_to_the_dead_letter_queue() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<LapinBroker>()
            .message(&declined())
            .publish()
            .await?;

        tb.broker::<LapinBroker>()
            .subscriber("payments")
            .assert_called(3)
            .with(&declined());
        tb.broker::<LapinBroker>()
            .published::<Payment>("payments")
            .assert_called(3)
            .with_header(RETRY_COUNT_HEADER, "2");
        tb.broker::<LapinBroker>()
            .published::<Payment>("payments.dead")
            .assert_called_once()
            .with(&declined())
            .with_header(RETRY_COUNT_HEADER, "3");
        tb.broker::<LapinBroker>()
            .subscriber("payments.dead")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }
}
