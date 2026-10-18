//! The service of `examples/lapin_transactions_scope.rs` written without `#[subscriber]`: the
//! body implements `Handle` over a slot whose publisher is transactional.
//!
//! ```text
//! cargo test --example manual_lapin_transactions_scope --features testing
//! cargo run --example manual_lapin_transactions_scope -- run
//! ```

use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_lapin::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
    items: Vec<String>,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "shipments")]
struct Shipment {
    order_id: u64,
    item: String,
}

// --8<-- [start:handler]
/// One shipment per item, in one transaction. An item without a name aborts it, and the shipments
/// already published are discarded with it.
struct Ship;

impl<Egress> Handle<Order, (), Outs<(Egress,)>> for Ship
where
    Egress: OutEntry<DefaultSlot, Wire: TransactionalPublisher>,
{
    async fn handle(
        &self,
        order: &Order,
        outs: &Outs<(Egress,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let scope = outs
            .get(DefaultSlot)
            .begin()
            .await
            .map_err(|_| HandlerOutcome::retry())?;
        for item in &order.items {
            if item.is_empty() {
                tracing::warn!(order.id, "an item without a name, nothing shipped");
                scope.abort().await.map_err(|_| HandlerOutcome::retry())?;
                return Err(HandlerOutcome::drop());
            }
            let shipment = Shipment {
                order_id: order.id,
                item: item.clone(),
            };
            if scope.message(&shipment).publish().await.is_err() {
                return Err(HandlerOutcome::retry());
            }
        }
        scope.commit().await.map_err(|_| HandlerOutcome::retry())
    }
}
// --8<-- [end:handler]

// --8<-- [start:app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        LapinBroker::new("amqp://localhost:5672"),
        |b| {
            // The confirms policy; `LapinPublish::default().server_tx()` is the other one.
            b.include(subscriber(RabbitQueue::new("orders"), Ship).build())
                .out(DefaultSlot, TransactionalPublish::default())
                .build();
        },
    )
}
// --8<-- [end:app]

fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_lapin::prelude::*;

    use super::{Order, Shipment, app};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_committed_order_ships_every_item() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let order = Order {
            id: 7,
            items: vec!["tea".into(), "cups".into()],
        };
        tb.broker::<LapinBroker>().message(&order).publish().await?;

        let shipped = tb
            .broker::<LapinBroker>()
            .published::<Shipment>("shipments")
            .assert_called(2)
            .decoded();
        assert_eq!(shipped[0].item, "tea");
        assert_eq!(shipped[1].item, "cups");
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_aborted_order_ships_nothing() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let order = Order {
            id: 8,
            items: vec!["tea".into(), String::new()],
        };
        tb.broker::<LapinBroker>().message(&order).publish().await?;

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
}
