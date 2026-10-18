//! A publish policy aimed at a named exchange: the message name is the routing key, and the
//! exchange routes it to every queue whose binding matches.
//!
//! ```text
//! cargo test --example lapin_publishing_exchange --features testing
//! cargo run --example lapin_publishing_exchange -- run
//! ```

use ruststream_lapin::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "order.placed")]
struct OrderPlaced {
    id: u64,
}

// --8<-- [start:exchange]
/// Every accepted order is announced on the `events` exchange under `order.placed`.
#[subscriber(RabbitQueue::new("orders"))]
async fn accept(order: &Order, Out(events): Out<impl Publisher>) -> HandlerOutcome {
    let placed = OrderPlaced { id: order.id };
    if events.message(&placed).publish().await.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

/// The audit service's queue takes every `order.*` event from that exchange.
#[subscriber(RabbitQueue::new("orders.audit").bind(RabbitExchange::topic("events"), "order.*"))]
async fn audit(placed: &OrderPlaced) -> HandlerOutcome {
    tracing::info!(placed.id, "order placed");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    let broker = LapinBroker::new("amqp://localhost:5672").declare_topology(true);
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(accept)
            .out(DefaultSlot, Publish::default().exchange("events"))
            .build();
        b.include(audit);
    })
}
// --8<-- [end:exchange]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_lapin::prelude::*;

    use super::{Order, OrderPlaced, app};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_event_reaches_the_bound_queue() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<LapinBroker>()
            .message(&Order { id: 7 })
            .publish()
            .await?;

        tb.broker::<LapinBroker>()
            .subscriber("orders.audit")
            .assert_called_once()
            .with(&OrderPlaced { id: 7 });
        tb.shutdown().await?;
        Ok(())
    }
}
