//! The orders service of the Reliable publishing guide, written without the macros: each handler
//! is a type with an `impl Handle`, mounted with the `subscriber` constructor, and `main` is
//! written by hand.
//!
//! ```text
//! docker compose -f docker-compose.test.yml up -d rabbitmq
//! cargo run --example manual_guide_publishing --features testing,ruststream/logging -- run
//! ```

use std::error::Error;
use std::process::ExitCode;
use std::time::Duration;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_lapin::prelude::*;
use serde::{Deserialize, Serialize};

// --8<-- [start:confirms]
#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
    parcels: Vec<String>,
    expedited: bool,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "shipments")]
struct Shipment {
    order_id: u64,
    parcel: String,
    expedited: bool,
}

/// Publishes one shipment per parcel: every one of them, or none.
async fn ship_all(
    shipments: &impl TransactionalPublisher,
    order: &Order,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let scope = shipments.begin().await?;
    for parcel in &order.parcels {
        if parcel.is_empty() {
            scope.abort().await?;
            return Err("a parcel without a description".into());
        }
        let shipment = Shipment {
            order_id: order.id,
            parcel: parcel.clone(),
            expedited: order.expedited,
        };
        scope.message(&shipment).publish().await?;
    }
    // Resolves once RabbitMQ confirmed every shipment of the order.
    scope.commit().await?;
    Ok(())
}

struct PackOrder;

impl<Shipments> Handle<Order, (), Outs<(Shipments,)>> for PackOrder
where
    Shipments: OutEntry<DefaultSlot, Wire: TransactionalPublisher>,
{
    async fn handle(
        &self,
        order: &Order,
        outs: &Outs<(Shipments,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        ship_all(outs.get(DefaultSlot), order).await.map_err(|err| {
            tracing::warn!(order.id, %err, "order not shipped");
            HandlerOutcome::drop()
        })
    }
}
// --8<-- [end:confirms]

// --8<-- [start:properties]
#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "pickups")]
struct Pickup {
    order_id: u64,
    parcel: String,
}

/// The slot pickups leave through.
#[derive(OutSlot)]
#[publishes(Pickup)]
struct Pickups;

/// An expedited pickup jumps the couriers' queue and is worthless after an hour.
struct BookCourier;

impl<Couriers> Handle<Shipment, (), Outs<(Couriers,)>> for BookCourier
where
    Couriers: OutEntry<Pickups, Wire: Publisher<Options = LapinPublishOptions>>,
{
    async fn handle(
        &self,
        shipment: &Shipment,
        outs: &Outs<(Couriers,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let pickups = outs.get(Pickups);
        let pickup = Pickup {
            order_id: shipment.order_id,
            parcel: shipment.parcel.clone(),
        };
        let sent = if shipment.expedited {
            pickups
                .message(&pickup)
                .priority(9)
                .expiration(Duration::from_hours(1))
                .publish()
                .await
        } else {
            pickups.message(&pickup).publish().await
        };
        sent.map_err(|_| HandlerOutcome::retry())
    }
}
// --8<-- [end:properties]

// --8<-- [start:app]
fn app() -> impl App {
    let broker = LapinBroker::new("amqp://localhost:5672").declare_topology(true);
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        // Confirms: a publish resolves once RabbitMQ has taken the message.
        b.include(subscriber("orders", PackOrder).build())
            .out(DefaultSlot, TransactionalPublish::default())
            .build();
        // Every pickup leaves at priority 3 unless the handler says otherwise.
        b.include(subscriber(RabbitQueue::new("shipments"), BookCourier).build())
            .out(Pickups, Publish::default().priority(3))
            .build();
    })
}

fn main() -> ExitCode {
    run_main(app)
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::Duration;

    use ruststream::testing::TestApp;
    use ruststream_lapin::prelude::*;

    use super::{Order, Pickups, Shipment, app};

    fn order(id: u64, parcels: &[&str], expedited: bool) -> Order {
        Order {
            id,
            parcels: parcels.iter().map(|parcel| (*parcel).to_owned()).collect(),
            expedited,
        }
    }

    #[tokio::test]
    async fn an_order_ships_every_parcel() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<LapinBroker>()
            .message(&order(1, &["kettle", "teapot"], false))
            .to("orders")
            .publish()
            .await?;

        tb.broker::<LapinBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        let shipped = tb
            .broker::<LapinBroker>()
            .published::<Shipment>("shipments")
            .assert_called(2)
            .decoded();
        let parcels: Vec<&str> = shipped.iter().map(|s| s.parcel.as_str()).collect();
        assert_eq!(parcels, ["kettle", "teapot"]);
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn an_order_with_a_bad_parcel_ships_nothing() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<LapinBroker>()
            .message(&order(2, &["kettle", ""], false))
            .to("orders")
            .publish()
            .await?;

        tb.broker::<LapinBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        tb.broker::<LapinBroker>()
            .published::<Shipment>("shipments")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn an_expedited_pickup_carries_its_priority_and_ttl() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<LapinBroker>()
            .message(&order(3, &["kettle"], true))
            .to("orders")
            .publish()
            .await?;

        tb.out::<Pickups>()
            .assert_called_once()
            .with_options(&LapinPublishOptions {
                priority: Some(9),
                expiration: Some(Duration::from_hours(1)),
                persistent: None,
            });
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_standard_pickup_takes_the_policy_priority() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<LapinBroker>()
            .message(&order(4, &["kettle"], false))
            .to("orders")
            .publish()
            .await?;

        tb.out::<Pickups>()
            .assert_called_once()
            .assert_options_default();
        tb.shutdown().await?;
        Ok(())
    }
}
