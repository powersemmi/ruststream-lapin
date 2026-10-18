//! A batch handler: deliveries collect on the client up to the mount site's size, or until
//! `batch_wait` passes since the first of them.
//!
//! ```text
//! cargo test --example lapin_subscriptions_batches --features testing
//! ```

use std::time::Duration;

use serde::{Deserialize, Serialize};

// --8<-- [start:batches]
use ruststream_lapin::prelude::*;

/// Settles up to 32 payments in one call; a batch that does not fill closes 200 ms after its
/// first payment arrived.
#[subscriber(RabbitQueue::new("payments")
    .prefetch(nonzero!(64))
    .batch_wait(Duration::from_millis(200)))]
async fn settle(payments: &[Payment]) -> HandlerOutcome {
    let cents: u64 = payments.iter().map(|payment| payment.cents).sum();
    tracing::info!(count = payments.len(), cents, "payments settled");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App<State = ()> {
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(
        LapinBroker::new("amqp://localhost:5672"),
        |b| {
            b.include(settle.batch(nonzero!(32)));
        },
    )
}
// --8<-- [end:batches]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "payments")]
struct Payment {
    id: u64,
    cents: u64,
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::Duration;

    use ruststream::testing::TestApp;
    use ruststream_lapin::prelude::*;

    use super::{Payment, app};

    // The harness's own publish waits for each reaction, so the test publishes through a bare
    // publisher: the three payments are on the queue together, and the batch takes all of them.
    #[tokio::test(start_paused = true)]
    async fn payments_waiting_together_settle_in_one_batch() -> Result<(), Box<dyn Error>> {
        let mut publisher = None;
        let tb = TestApp::with_state(app(), |brokers| {
            publisher = Some(
                brokers
                    .broker::<LapinBroker>()
                    .publisher(Publish::default()),
            );
        })
        .await?;
        let publisher = publisher.ok_or("the state builder ran")?;
        for id in 1..=3 {
            publisher
                .message(&Payment { id, cents: 100 })
                .publish()
                .await?;
        }
        tb.advance(Duration::from_millis(200)).await?;

        tb.broker::<LapinBroker>()
            .subscriber("payments")
            .assert_called_once()
            .assert_batch_sizes(&[3])
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }
}
