//! The service of `examples/lapin_broker_errors.rs` without the macros: the handler is a named
//! type, the slot a marker type, and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_lapin_broker_errors --features testing
//! ```

use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use ruststream_lapin::prelude::*;

/// The slot shipments leave through.
struct Shipments;

impl OutSlot for Shipments {
    const NAME: &'static str = "Shipments";
    // `Shipment` fixes its destination, so no transform on this slot may name one.
    type Destination = Reads;
}

impl PublishedThrough<Shipments> for Shipment {}

// --8<-- [start:handler]
struct Ship;

impl<Entry> Handle<Order, (), Outs<(Entry,)>> for Ship
where
    Entry: OutEntry<Shipments, Wire: Publisher>,
{
    async fn handle(
        &self,
        order: &Order,
        outs: &Outs<(Entry,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let shipment = Shipment {
            order_id: order.id,
            labels: (1..=order.parcels)
                .map(|parcel| format!("{}-{parcel:07}", order.id))
                .collect(),
        };
        if let Err(err) = outs.get(Shipments).message(&shipment).publish().await {
            // The error names the step that failed and the server's reason.
            tracing::warn!(subscription = "orders", order.id, error = %err, "shipment not published");
            return Err(HandlerOutcome::drop());
        }
        Ok(())
    }
}

fn orders() -> RabbitQueue {
    RabbitQueue::new("orders").dead_letter_exchange("orders.failed")
}
// --8<-- [end:handler]

fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        LapinBroker::new("amqp://localhost:5672"),
        |b| {
            b.include(subscriber(orders(), Ship).build())
                .out(Shipments, Publish::default())
                .build();
        },
    )
}

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
    parcels: u32,
}

// `#[outgoing(name = "orders")]` by hand: an order names the queue it is published to.
impl OutgoingDestination for Order {
    type Form = FixedName;
    const DESTINATION: &'static str = "orders";
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Shipment {
    order_id: u64,
    labels: Vec<String>,
}

// `#[outgoing(name = "shipments")]` by hand: a shipment names the queue it is published to.
impl OutgoingDestination for Shipment {
    type Form = FixedName;
    const DESTINATION: &'static str = "shipments";
}

impl MessageHeaders for Shipment {
    type Contract = NoHeaders;
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_lapin::AmqpError;
    use ruststream_lapin::prelude::*;

    use std::future::ready;

    use super::{Order, Shipment, app};

    /// A quorum queue takes no priority argument, so a server refuses to declare this one.
    struct Prioritised;

    impl Handle<Order> for Prioritised {
        fn handle(
            &self,
            order: &Order,
            _outs: &(),
            _ctx: &mut Context<'_>,
        ) -> impl Future<Output = Result<(), HandlerOutcome>> {
            tracing::info!(order.id, "prioritised order");
            ready(Ok(()))
        }
    }

    fn prioritised() -> RabbitQuorumQueue {
        RabbitQuorumQueue::new("orders.prioritised")
            .argument("x-max-priority", AMQPValue::LongLongInt(5))
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
            b.include(subscriber(prioritised(), Prioritised).build());
        });

        let failed = TestApp::start(app).await.err();
        let mut cause: Option<&(dyn Error + 'static)> =
            failed.as_ref().map(|err| -> &(dyn Error + 'static) { err });
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
