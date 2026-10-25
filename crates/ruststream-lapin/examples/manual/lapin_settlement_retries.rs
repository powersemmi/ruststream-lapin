//! The service of `examples/lapin_settlement_retries.rs` written without `#[subscriber]`: each
//! body implements `Handle`, and the cap is the same steps of the mount chain.
//!
//! ```text
//! cargo test --example manual_lapin_settlement_retries --features testing
//! cargo run --example manual_lapin_settlement_retries -- run
//! ```

use std::future::ready;
use std::process::ExitCode;

use ruststream::nonzero;
use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_lapin::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "payments")]
struct Payment {
    id: u64,
    card: String,
}

// --8<-- [start:retries]
/// A card the gateway declines is asked for again, at once.
struct Charge;

impl Handle<Payment> for Charge {
    fn handle(
        &self,
        payment: &Payment,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        if payment.card.starts_with("0000") {
            tracing::warn!(payment.id, "card declined, retrying");
            return ready(Err(HandlerOutcome::retry()));
        }
        ready(Ok(()))
    }
}

/// The payments that ran out of deliveries.
struct Bury;

impl Handle<Payment> for Bury {
    fn handle(
        &self,
        payment: &Payment,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::error!(payment.id, "payment dead-lettered");
        ready(Ok(()))
    }
}

fn app() -> impl App {
    // The cap becomes the queue's own arguments, so this service declares the queue.
    let broker = LapinBroker::new("amqp://localhost:5672").declare_topology(true);
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(broker, |b| {
        // Three deliveries, the first included, then the queue moves the payment to
        // `payments.dead`.
        b.include(subscriber(RabbitQuorumQueue::new("payments"), Charge).build())
            .max_attempts(nonzero!(3u32))
            .dead_letter("payments.dead");
        b.include(subscriber("payments.dead", Bury).build());
    })
}
// --8<-- [end:retries]

fn main() -> ExitCode {
    run_main(app)
}

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
