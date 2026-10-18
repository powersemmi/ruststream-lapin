//! The service of `examples/lapin_subscriptions_batches.rs` without the macros: the batch handler is a named type over a slice, and `main` is
//! written by hand.
//!
//! ```text
//! cargo test --example manual_lapin_subscriptions_batches --features testing
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;
use std::time::Duration;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;

use serde::{Deserialize, Serialize};

// --8<-- [start:batches]
use ruststream_lapin::prelude::*;

/// Settles a batch of payments; `Ok` acknowledges the whole batch.
struct Settle;

impl Handle<[Payment]> for Settle {
    fn handle(
        &self,
        payments: &[Payment],
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), Vec<HandlerOutcome>>> {
        let cents: u64 = payments.iter().map(|payment| payment.cents).sum();
        tracing::info!(count = payments.len(), cents, "payments settled");
        ready(Ok(()))
    }
}

fn app() -> impl App<State = ()> {
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(
        LapinBroker::new("amqp://localhost:5672"),
        |b| {
            // Up to 32 payments per call; a batch that does not fill closes 200 ms after its
            // first payment arrived.
            let payments = RabbitQueue::new("payments")
                .prefetch(nonzero!(64))
                .batch_wait(Duration::from_millis(200));
            b.include(subscriber(payments, Settle).batch(nonzero!(32)).build());
        },
    )
}
// --8<-- [end:batches]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Payment {
    id: u64,
    cents: u64,
}

// `#[outgoing(name = "payments")]` by hand: the type names the queue it is published to.
impl OutgoingDestination for Payment {
    type Form = FixedName;
    const DESTINATION: &'static str = "payments";
}

impl MessageHeaders for Payment {
    type Contract = NoHeaders;
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
