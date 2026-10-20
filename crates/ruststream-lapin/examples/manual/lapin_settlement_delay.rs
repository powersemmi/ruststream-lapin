//! The service of `examples/lapin_settlement_delay.rs` written without `#[subscriber]`: the body
//! implements `Handle`, and the descriptor with its delay is the same value.
//!
//! ```text
//! cargo test --example manual_lapin_settlement_delay --features testing
//! cargo run --example manual_lapin_settlement_delay -- run
//! ```

use std::future::ready;
use std::process::ExitCode;
use std::time::Duration;

use ruststream::runtime::RETRY_COUNT_HEADER;
use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_lapin::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "shipments")]
struct Shipment {
    id: u64,
}

/// How long a shipment waits for the warehouse to open.
const WAIT: Duration = Duration::from_secs(60);

// --8<-- [start:delay]
/// A shipment that arrives while the warehouse is closed waits a minute in `shipments.retry`, on
/// the broker. The copy it releases carries the retry count, and the shipment goes out.
struct Dispatch;

impl Handle<Shipment> for Dispatch {
    fn handle(
        &self,
        shipment: &Shipment,
        _outs: &(),
        ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        if ctx.headers().get_str(RETRY_COUNT_HEADER).is_none() {
            tracing::info!(shipment.id, "warehouse closed, waiting");
            return ready(Err(HandlerOutcome::retry_after(WAIT)));
        }
        tracing::info!(shipment.id, "dispatched");
        ready(Ok(()))
    }
}

fn app() -> impl App {
    // The service declares its queues, `shipments.retry` included.
    let broker = LapinBroker::new("amqp://localhost:5672").declare_topology(true);
    RustStream::new(AppInfo::new("shipping", "0.1.0")).with_broker(broker, |b| {
        let shipments = RabbitQueue::new("shipments").delay(Delay::dlx_ttl());
        b.include(subscriber(shipments, Dispatch).build());
    })
}
// --8<-- [end:delay]

fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_lapin::prelude::*;

    use super::{Shipment, WAIT, app};

    #[tokio::test(start_paused = true)]
    async fn the_waiting_queue_releases_the_copy() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<LapinBroker>()
            .message(&Shipment { id: 2 })
            .publish()
            .await?;

        tb.advance(WAIT).await?;

        tb.broker::<LapinBroker>()
            .subscriber("shipments")
            .assert_called(2)
            .settled(HandlerOutcome::ack());
        // The copy waited on the broker: the service published nothing to the queue itself.
        tb.broker::<LapinBroker>()
            .published::<Shipment>("shipments")
            .assert_called_once();
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn nothing_comes_back_before_the_ttl() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<LapinBroker>()
            .message(&Shipment { id: 2 })
            .publish()
            .await?;

        tb.advance(WAIT / 2).await?;

        tb.broker::<LapinBroker>()
            .subscriber("shipments")
            .assert_called_once()
            .settled(HandlerOutcome::retry_after(WAIT));
        tb.shutdown().await?;
        Ok(())
    }
}
