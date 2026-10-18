//! `retry_after` on a queue with no delay of its own: the service holds the copy over the delay
//! and publishes it back to the queue, with the retry count raised by one.
//!
//! ```text
//! cargo test --example lapin_settlement_retry_after --features testing
//! cargo run --example lapin_settlement_retry_after -- run
//! ```

use std::time::Duration;

use ruststream::runtime::RETRY_COUNT_HEADER;
use ruststream_lapin::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "refunds")]
struct Refund {
    id: u64,
}

/// How long a refund whose payment has not settled yet waits before it is tried again.
const RECHECK: Duration = Duration::from_secs(30);

// --8<-- [start:retry_after]
/// The first delivery finds the payment unsettled and asks for the refund again in 30 seconds.
/// The copy that comes back carries the retry count, and the refund goes through.
#[subscriber(RabbitQueue::new("refunds"))]
async fn issue_refund(refund: &Refund, ctx: &mut Context<'_>) -> HandlerOutcome {
    if ctx.headers().get_str(RETRY_COUNT_HEADER).is_none() {
        tracing::info!(refund.id, "payment not settled yet");
        return HandlerOutcome::retry_after(RECHECK);
    }
    tracing::info!(refund.id, "refunded");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("refunds", "0.1.0")).with_broker(
        LapinBroker::new("amqp://localhost:5672"),
        |b| {
            b.include(issue_refund);
        },
    )
}
// --8<-- [end:retry_after]

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
