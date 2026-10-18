//! A handler that publishes all-or-nothing: it opens a transaction scope on its slot, publishes
//! one shipment per item and commits, or aborts and publishes none.
//!
//! ```text
//! cargo test --example lapin_transactions_scope --features testing
//! cargo run --example lapin_transactions_scope -- run
//! ```

use ruststream_lapin::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
    items: Vec<String>,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "shipments")]
struct Shipment {
    order_id: u64,
    item: String,
}

// --8<-- [start:handler]
/// One shipment per item, in one transaction. An item without a name aborts it, and the shipments
/// already published are discarded with it.
#[subscriber(RabbitQueue::new("orders"))]
async fn ship(order: &Order, Out(shipments): Out<impl TransactionalPublisher>) -> HandlerOutcome {
    let Ok(scope) = shipments.begin().await else {
        return HandlerOutcome::retry();
    };
    for item in &order.items {
        if item.is_empty() {
            tracing::warn!(order.id, "an item without a name, nothing shipped");
            return match scope.abort().await {
                Ok(()) => HandlerOutcome::drop(),
                Err(_) => HandlerOutcome::retry(),
            };
        }
        let shipment = Shipment {
            order_id: order.id,
            item: item.clone(),
        };
        if scope.message(&shipment).publish().await.is_err() {
            return HandlerOutcome::retry();
        }
    }
    if scope.commit().await.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}
// --8<-- [end:handler]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        LapinBroker::new("amqp://localhost:5672"),
        |b| {
            // The confirms policy; `LapinPublish::default().server_tx()` is the other one.
            b.include(ship)
                .out(DefaultSlot, TransactionalPublish::default())
                .build();
        },
    )
}
// --8<-- [end:app]

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
