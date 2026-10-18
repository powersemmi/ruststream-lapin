//! Per-message AMQP properties from a handler: every shipment carries the priority the mount site
//! fixed, and an expedited one leaves at a higher priority with a TTL of its own.
//!
//! ```text
//! cargo test --example lapin_publishing_properties --features testing
//! cargo run --example lapin_publishing_properties -- run
//! ```

use std::time::Duration;

use ruststream::OutSlot;
use ruststream_lapin::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
    expedited: bool,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "shipments")]
struct Shipment {
    order_id: u64,
}

// --8<-- [start:steps]
#[derive(OutSlot)]
#[publishes(Shipment)]
struct Shipments;

/// An expedited order leaves at priority 9 and expires after an hour. An ordinary one takes the
/// policy's defaults.
#[subscriber(RabbitQueue::new("orders"))]
async fn ship(
    order: &Order,
    Out(shipments): Out<impl Publisher<Options = LapinPublishOptions>, Shipments>,
) -> HandlerOutcome {
    let shipment = Shipment { order_id: order.id };
    let sent = if order.expedited {
        shipments
            .message(&shipment)
            .priority(9)
            .expiration(Duration::from_hours(1))
            .publish()
            .await
    } else {
        shipments.message(&shipment).publish().await
    };
    if sent.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        LapinBroker::new("amqp://localhost:5672"),
        |b| {
            b.include(ship)
                .out(Shipments, Publish::default().priority(3))
                .build();
        },
    )
}
// --8<-- [end:steps]

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::Duration;

    use ruststream::testing::TestApp;
    use ruststream_lapin::prelude::*;

    use super::{Order, Shipment, Shipments, app};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_expedited_shipment_carries_what_the_steps_asked_for() -> Result<(), Box<dyn Error>>
    {
        let tb = TestApp::start(app()).await?;
        tb.broker::<LapinBroker>()
            .message(&Order {
                id: 7,
                expedited: true,
            })
            .publish()
            .await?;

        tb.out::<Shipments>()
            .assert_called_once()
            .with_options(&LapinPublishOptions {
                priority: Some(9),
                expiration: Some(Duration::from_hours(1)),
                persistent: None,
            });
        tb.broker::<LapinBroker>()
            .published::<Shipment>("shipments")
            .assert_called_once()
            .with(&Shipment { order_id: 7 });
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_ordinary_shipment_takes_the_policy_defaults() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<LapinBroker>()
            .message(&Order {
                id: 8,
                expedited: false,
            })
            .publish()
            .await?;

        tb.out::<Shipments>()
            .assert_called_once()
            .assert_options_default();
        tb.shutdown().await?;
        Ok(())
    }
}
