//! The service of `examples/lapin_settlement_retry_after.rs` written without `#[subscriber]`: the
//! body implements `Handle` and answers with the same outcome.
//!
//! ```text
//! cargo test --example manual_lapin_settlement_retry_after --features testing
//! cargo run --example manual_lapin_settlement_retry_after -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;
use std::time::Duration;

use ruststream::runtime::RETRY_COUNT_HEADER;
use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_lapin::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "refunds")]
struct Refund {
    id: u64,
}

/// How long a refund whose payment has not settled yet waits before it is tried again.
const RECHECK: Duration = Duration::from_secs(30);

// --8<-- [start:retry_after]
/// The first delivery finds the payment unsettled and asks for the refund again in 30 seconds.
/// The copy that comes back carries the retry count, and the refund goes through.
struct IssueRefund;

impl Handle<Refund> for IssueRefund {
    fn handle(
        &self,
        refund: &Refund,
        _outs: &(),
        ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        if ctx.headers().get_str(RETRY_COUNT_HEADER).is_none() {
            tracing::info!(refund.id, "payment not settled yet");
            return ready(Err(HandlerOutcome::retry_after(RECHECK)));
        }
        tracing::info!(refund.id, "refunded");
        ready(Ok(()))
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("refunds", "0.1.0")).with_broker(
        LapinBroker::new("amqp://localhost:5672"),
        |b| {
            b.include(subscriber(RabbitQueue::new("refunds"), IssueRefund).build());
        },
    )
}
// --8<-- [end:retry_after]

fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::runtime::RETRY_COUNT_HEADER;
    use ruststream::testing::TestApp;
    use ruststream_lapin::prelude::*;

    use super::{RECHECK, Refund, app};

    #[tokio::test(start_paused = true)]
    async fn the_copy_comes_back_after_the_delay() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<LapinBroker>()
            .message(&Refund { id: 4 })
            .publish()
            .await?;
        tb.broker::<LapinBroker>()
            .subscriber("refunds")
            .assert_called_once()
            .settled(HandlerOutcome::retry_after(RECHECK));

        tb.advance(RECHECK).await?;

        tb.broker::<LapinBroker>()
            .subscriber("refunds")
            .assert_called(2)
            .settled(HandlerOutcome::ack());
        // The service published the copy itself, back under the queue's name.
        tb.broker::<LapinBroker>()
            .published::<Refund>("refunds")
            .assert_called(2)
            .with_header(RETRY_COUNT_HEADER, "1");
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn nothing_comes_back_before_the_delay() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<LapinBroker>()
            .message(&Refund { id: 4 })
            .publish()
            .await?;

        tb.advance(RECHECK / 2).await?;

        tb.broker::<LapinBroker>()
            .subscriber("refunds")
            .assert_called_once();
        tb.shutdown().await?;
        Ok(())
    }
}
