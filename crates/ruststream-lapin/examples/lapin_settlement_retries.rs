//! A retry cap on a quorum queue: `max_attempts` and `dead_letter` at the mount site become the
//! queue's delivery limit and dead-letter route, and the server moves the spent message.
//!
//! ```text
//! cargo test --example lapin_settlement_retries --features testing
//! cargo run --example lapin_settlement_retries -- run
//! ```

use ruststream::nonzero;
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
#[subscriber(RabbitQuorumQueue::new("payments"))]
async fn charge(payment: &Payment) -> HandlerOutcome {
    if payment.card.starts_with("0000") {
        tracing::warn!(payment.id, "card declined, retrying");
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

/// The payments that ran out of deliveries.
#[subscriber("payments.dead")]
async fn bury(payment: &Payment) -> HandlerOutcome {
    tracing::error!(payment.id, "payment dead-lettered");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    // The cap becomes the queue's own arguments, so this service declares the queue.
    let broker = LapinBroker::new("amqp://localhost:5672").declare_topology(true);
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(broker, |b| {
        // Three deliveries, the first included, then the queue moves the payment to
        // `payments.dead`.
        b.include(charge)
            .max_attempts(nonzero!(3u32))
            .dead_letter("payments.dead");
        b.include(bury);
    })
}
// --8<-- [end:retries]

#[cfg(test)]
mod tests {
    use std::error::Error;

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
        // The queue moved the payment: the service published nothing to the dead-letter queue.
        tb.broker::<LapinBroker>()
            .published::<Payment>("payments.dead")
            .assert_not_called();
        tb.broker::<LapinBroker>()
            .subscriber("payments.dead")
            .assert_called_once()
            .with(&declined())
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }
}
