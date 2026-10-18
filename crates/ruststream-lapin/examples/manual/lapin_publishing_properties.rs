//! The service of `examples/lapin_publishing_properties.rs` written without `#[subscriber]`: the
//! body implements `Handle` over a slot whose publisher takes this crate's options.
//!
//! ```text
//! cargo test --example manual_lapin_publishing_properties --features testing
//! cargo run --example manual_lapin_publishing_properties -- run
//! ```

use std::process::ExitCode;
use std::time::Duration;

use ruststream::OutSlot;
use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_lapin::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
    expedited: bool,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
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
struct Ship;

impl<Egress> Handle<Order, (), Outs<(Egress,)>> for Ship
where
    Egress: OutEntry<Shipments, Wire: Publisher<Options = LapinPublishOptions>>,
{
    async fn handle(
        &self,
        order: &Order,
        outs: &Outs<(Egress,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let shipment = Shipment { order_id: order.id };
        let shipments = outs.get(Shipments);
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
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        LapinBroker::new("amqp://localhost:5672"),
        |b| {
            b.include(subscriber(RabbitQueue::new("orders"), Ship).build())
                .out(Shipments, Publish::default().priority(3))
                .build();
        },
    )
}
// --8<-- [end:steps]

fn main() -> ExitCode {
    run_main(app)
}

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
