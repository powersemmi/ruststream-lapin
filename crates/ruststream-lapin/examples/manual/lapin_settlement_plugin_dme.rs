//! The service of `examples/lapin_settlement_plugin_dme.rs` written without `#[subscriber]`: the
//! body implements `Handle`, and the descriptor with its plugin delay is the same value.
//!
//! ```text
//! cargo test --example manual_lapin_settlement_plugin_dme --features testing,plugin-dme
//! AMQP_PLUGINS_TEST_URL=amqp://127.0.0.1:5673 RUSTSTREAM_REQUIRE_LIVE=1 \
//!     cargo test --example manual_lapin_settlement_plugin_dme --features testing,plugin-dme
//! ```

use std::future::ready;
use std::process::ExitCode;
use std::time::Duration;

use ruststream::runtime::RETRY_COUNT_HEADER;
use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_lapin::prelude::*;
use serde::{Deserialize, Serialize};

#[cfg(test)]
#[path = "../../tests/live/mod.rs"]
mod live;

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Reminder {
    id: u64,
    after_secs: u64,
}

// --8<-- [start:dme]
/// Each reminder waits as long as it asks, in the plugin's exchange `reminders.delay`. A short
/// wait is released on time even while a long one is still held.
struct Remind;

impl Handle<Reminder> for Remind {
    fn handle(
        &self,
        reminder: &Reminder,
        _outs: &(),
        ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        if ctx.headers().get_str(RETRY_COUNT_HEADER).is_none() {
            let wait = Duration::from_secs(reminder.after_secs);
            return ready(Err(HandlerOutcome::retry_after(wait)));
        }
        tracing::info!(reminder.id, "reminder sent");
        ready(Ok(()))
    }
}

fn app() -> impl App {
    // The service declares the delayed exchange; the plugin has to be enabled on the server.
    let broker = LapinBroker::new("amqp://localhost:5672").declare_topology(true);
    RustStream::new(AppInfo::new("reminders", "0.1.0")).with_broker(broker, |b| {
        let reminders = RabbitQueue::new("reminders").delay(Delay::plugin_dme());
        b.include(subscriber(reminders, Remind).build());
    })
}
// --8<-- [end:dme]

fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::process;
    use std::time::Duration;

    use ruststream::testing::TestApp;
    use ruststream_lapin::prelude::*;

    use super::{Remind, Reminder, app, live};

    /// The body both modes run: the copy is held for its delay, then delivered again.
    async fn holds_then_redelivers<State: Send + Sync + 'static>(
        tb: TestApp<State>,
        queue: &str,
    ) -> Result<(), Box<dyn Error>> {
        let reminder = Reminder {
            id: 1,
            after_secs: 1,
        };
        tb.broker::<LapinBroker>()
            .message(&reminder)
            .to(queue)
            .publish()
            .await?;
        tb.broker::<LapinBroker>()
            .subscriber(queue)
            .assert_called_once()
            .settled(HandlerOutcome::retry_after(Duration::from_secs(1)));

        tb.advance(Duration::from_secs(1)).await?;

        tb.broker::<LapinBroker>()
            .subscriber(queue)
            .assert_called(2)
            .with(&reminder)
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn in_process() -> Result<(), Box<dyn Error>> {
        holds_then_redelivers(TestApp::start(app()).await?, "reminders").await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn against_the_plugin_stand() -> Result<(), Box<dyn Error>> {
        let Some(url) = live::url("AMQP_PLUGINS_TEST_URL") else {
            return Ok(());
        };
        // A queue of this run's own, which takes its delayed exchange's name with it.
        let queue = format!("reminders-{}", process::id());
        let source = RabbitQueue::new(queue.clone())
            .auto_delete(true)
            .delay(Delay::plugin_dme());
        let broker = LapinBroker::new(url).declare_topology(true);
        let app = RustStream::new(AppInfo::new("reminders", "0.1.0")).with_broker(broker, |b| {
            b.include(subscriber(source, Remind).build());
        });
        let tb = TestApp::start_live_within(app, Duration::from_secs(30)).await?;
        holds_then_redelivers(tb, &queue).await
    }
}
