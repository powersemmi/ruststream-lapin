//! A quorum queue ends a poison loop itself: `delivery_limit` becomes its `x-delivery-limit`, and
//! a message that spent its deliveries leaves through the dead-letter exchange.
//!
//! ```text
//! cargo test --example lapin_settlement_delivery_limit --features testing
//! cargo run --example lapin_settlement_delivery_limit -- run
//! ```

use ruststream_lapin::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "charges")]
struct Charge {
    id: u64,
}

// --8<-- [start:limit]
/// The provider refuses this charge every time. The queue returns it four times, so it is
/// delivered five times, and then the queue moves it to `charges.dlx`.
#[subscriber(RabbitQuorumQueue::new("charges")
    .delivery_limit(4)
    .dead_letter_exchange("charges.dlx"))]
async fn take_payment(charge: &Charge) -> HandlerOutcome {
    tracing::warn!(charge.id, "provider refused the charge");
    HandlerOutcome::retry()
}

/// The charges the queue gave up on.
#[subscriber(RabbitQueue::new("charges.parked").bind(RabbitExchange::fanout("charges.dlx"), ""))]
async fn park(charge: &Charge) -> HandlerOutcome {
    tracing::error!(charge.id, "charge parked");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    let broker = LapinBroker::new("amqp://localhost:5672").declare_topology(true);
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(broker, |b| {
        b.include(take_payment);
        b.include(park);
    })
}
// --8<-- [end:limit]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_lapin::prelude::*;

    use super::{Charge, app};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_fifth_delivery_is_the_last() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<LapinBroker>()
            .message(&Charge { id: 5 })
            .publish()
            .await?;

        tb.broker::<LapinBroker>()
            .subscriber("charges")
            .assert_called(5)
            .settled(HandlerOutcome::retry());
        tb.broker::<LapinBroker>()
            .subscriber("charges.parked")
            .assert_called_once()
            .with(&Charge { id: 5 });
        // The queue moved the charge; the service published nothing.
        tb.broker::<LapinBroker>()
            .published::<Charge>("charges")
            .assert_called_once();
        tb.shutdown().await?;
        Ok(())
    }
}
