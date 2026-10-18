//! The service of `examples/lapin_settlement_retries.rs` written without `#[subscriber]`: each
//! body implements `Handle`, and the cap is the same steps of the mount chain.
//!
//! ```text
//! cargo test --example manual_lapin_settlement_retries --features testing
//! cargo run --example manual_lapin_settlement_retries -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::nonzero;
use ruststream::runtime::RETRY_COUNT_HEADER;
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

/// The payments that ran out of deliveries, with the count the runtime kept.
struct Bury;

impl Handle<Payment> for Bury {
    fn handle(
        &self,
        payment: &Payment,
        _outs: &(),
        ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        let deliveries = ctx.headers().get_str(RETRY_COUNT_HEADER).unwrap_or("1");
        tracing::error!(payment.id, deliveries, "payment dead-lettered");
        ready(Ok(()))
    }
}

fn app() -> impl App {
    let broker = LapinBroker::new("amqp://localhost:5672");
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(broker, |b| {
        // Three deliveries, the first included, then the payment goes to `payments.dead`. The
        // copies leave through the confirms policy, which waits for the broker to confirm each.
        b.include(subscriber(RabbitQueue::new("payments"), Charge).build())
            .max_attempts(nonzero!(3u32))
            .dead_letter("payments.dead")
            .out_retry(TransactionalPublish::default());
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
