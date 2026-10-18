//! A subscription by queue name: the classic queue `orders`, consumed with the descriptor
//! defaults.
//!
//! ```text
//! cargo test --example lapin_subscriptions_queue --features testing
//! ```

use serde::{Deserialize, Serialize};

// --8<-- [start:handler]
use ruststream_lapin::prelude::*;

#[subscriber("orders")]
async fn place(order: &Order) -> HandlerOutcome {
    if order.items == 0 {
        tracing::warn!(order.id, "empty order dropped");
        return HandlerOutcome::drop();
    }
    tracing::info!(order.id, order.items, "order placed");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        LapinBroker::new("amqp://localhost:5672"),
        |b| {
            b.include(place);
        },
    )
}
// --8<-- [end:handler]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
    items: u32,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_lapin::prelude::*;

    use super::{Order, app};

    #[tokio::test]
    async fn an_order_with_items_is_acked() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<LapinBroker>()
            .message(&Order { id: 7, items: 2 })
            .publish()
            .await?;

        tb.broker::<LapinBroker>()
            .subscriber("orders")
            .assert_called_once()
            .with(&Order { id: 7, items: 2 })
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn an_empty_order_is_dropped() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<LapinBroker>()
            .message(&Order { id: 8, items: 0 })
            .publish()
            .await?;

        tb.broker::<LapinBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        tb.shutdown().await?;
        Ok(())
    }
}
