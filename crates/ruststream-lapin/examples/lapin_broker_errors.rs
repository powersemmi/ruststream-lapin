//! `AmqpError` where a service meets it: a publish the server refuses, and a startup that fails.
//!
//! ```text
//! cargo test --example lapin_broker_errors --features testing
//! ```

use serde::{Deserialize, Serialize};

use ruststream_lapin::prelude::*;

#[derive(OutSlot)]
#[publishes(Shipment)]
struct Shipments;

// --8<-- [start:handler]
#[subscriber(RabbitQueue::new("orders").dead_letter_exchange("orders.failed"))]
async fn ship(order: &Order, Out(shipments): Out<impl Publisher, Shipments>) -> HandlerOutcome {
    let shipment = Shipment {
        order_id: order.id,
        labels: (1..=order.parcels)
            .map(|parcel| format!("{}-{parcel:07}", order.id))
            .collect(),
    };
    if let Err(err) = shipments.message(&shipment).publish().await {
        // The error names the step that failed and the server's reason.
        tracing::warn!(subscription = "orders", order.id, error = %err, "shipment not published");
        return HandlerOutcome::drop();
    }
    HandlerOutcome::ack()
}
// --8<-- [end:handler]

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        LapinBroker::new("amqp://localhost:5672"),
        |b| {
            b.include(ship).out(Shipments, Publish::default()).build();
        },
    )
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
    parcels: u32,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "shipments")]
struct Shipment {
    order_id: u64,
    labels: Vec<String>,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_lapin::AmqpError;
    use ruststream_lapin::prelude::*;

    use super::{Order, Shipment, app};

    /// A quorum queue takes no priority argument, so a server refuses to declare this one.
    #[subscriber(RabbitQuorumQueue::new("orders.prioritised")
        .argument("x-max-priority", AMQPValue::LongLongInt(5)))]
    async fn prioritised(order: &Order) -> HandlerOutcome {
        tracing::info!(order.id, "prioritised order");
        HandlerOutcome::ack()
    }

    #[tokio::test]
    async fn a_shipment_the_server_takes_acks_the_order() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<LapinBroker>()
            .message(&Order { id: 7, parcels: 1 })
            .publish()
            .await?;

        tb.broker::<LapinBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.broker::<LapinBroker>()
            .published::<Shipment>("shipments")
            .assert_called_once()
            .with(&Shipment {
                order_id: 7,
                labels: vec!["7-0000001".to_owned()],
            });
        tb.shutdown().await?;
        Ok(())
    }

    // Over 16 MiB of labels: past the server's default `max_message_size`.
    #[tokio::test]
    async fn a_shipment_over_the_size_limit_drops_the_order() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<LapinBroker>()
            .message(&Order {
                id: 8,
                parcels: 1_500_000,
            })
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
    async fn a_failed_startup_carries_the_amqp_error() {
        let broker = LapinBroker::new("amqp://localhost:5672").declare_topology(true);
        let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
            b.include(prioritised);
        });

        let failed = TestApp::start(app).await.err();
        let mut cause: Option<&(dyn Error + 'static)> = failed.as_ref().map(|err| err as _);
        while let Some(err) = cause {
            if let Some(amqp) = err.downcast_ref::<AmqpError>() {
                assert!(matches!(amqp, AmqpError::Declare(_)), "{amqp}");
                return;
            }
            cause = err.source();
        }
        panic!("startup did not fail with an AmqpError: {failed:?}");
    }
}
