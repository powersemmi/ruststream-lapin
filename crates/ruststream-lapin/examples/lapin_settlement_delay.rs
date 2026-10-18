//! `delay(Delay::dlx_ttl())` keeps a delayed copy on the broker: it waits in `<queue>.retry` under
//! a per-message TTL, and the waiting queue dead-letters it back when the TTL fires.
//!
//! ```text
//! cargo test --example lapin_settlement_delay --features testing
//! cargo run --example lapin_settlement_delay -- run
//! ```

use std::time::Duration;

use ruststream::runtime::RETRY_COUNT_HEADER;
use ruststream_lapin::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "shipments")]
struct Shipment {
    id: u64,
}

/// How long a shipment waits for the warehouse to open.
const WAIT: Duration = Duration::from_secs(60);

// --8<-- [start:delay]
/// A shipment that arrives while the warehouse is closed waits a minute in `shipments.retry`, on
/// the broker. The copy it releases carries the retry count, and the shipment goes out.
#[subscriber(RabbitQueue::new("shipments").delay(Delay::dlx_ttl()))]
async fn dispatch(shipment: &Shipment, ctx: &mut Context<'_>) -> HandlerOutcome {
    if ctx.headers().get_str(RETRY_COUNT_HEADER).is_none() {
        tracing::info!(shipment.id, "warehouse closed, waiting");
        return HandlerOutcome::retry_after(WAIT);
    }
    tracing::info!(shipment.id, "dispatched");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    // The service declares its queues, `shipments.retry` included.
    let broker = LapinBroker::new("amqp://localhost:5672").declare_topology(true);
    RustStream::new(AppInfo::new("shipping", "0.1.0")).with_broker(broker, |b| {
        b.include(dispatch);
    })
}
// --8<-- [end:delay]

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
