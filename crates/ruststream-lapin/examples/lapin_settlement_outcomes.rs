//! How a handler's answer settles an AMQP delivery: an ack sends `basic.ack`, a retry sends
//! `basic.reject` with requeue, and a drop sends `basic.reject` without it.
//!
//! ```text
//! cargo test --example lapin_settlement_outcomes --features testing
//! cargo run --example lapin_settlement_outcomes -- run
//! ```

use ruststream_lapin::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
    sku: String,
}

// --8<-- [start:outcomes]
/// An order without a SKU is rejected for good. Any other order is requeued once, the first time
/// it arrives, and acknowledged when the queue hands it out again marked redelivered.
#[subscriber(RabbitQueue::new("orders"))]
async fn reserve(order: &Order, Ctx(redelivered): Ctx<Redelivered>) -> HandlerOutcome {
    if order.sku.is_empty() {
        tracing::warn!(order.id, "order without a SKU, rejected");
        return HandlerOutcome::drop();
    }
    if !redelivered {
        tracing::info!(order.id, "stock not counted yet, back to the queue");
        return HandlerOutcome::retry();
    }
    tracing::info!(order.id, "reserved");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("stock", "0.1.0")).with_broker(
        LapinBroker::new("amqp://localhost:5672"),
        |b| {
            b.include(reserve);
        },
    )
}
// --8<-- [end:outcomes]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::{Outcome, TestApp};
    use ruststream_lapin::prelude::*;

    use super::{Order, app};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_retry_comes_back_redelivered_and_is_acked() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let order = Order {
            id: 7,
            sku: "widget".into(),
        };
        tb.broker::<LapinBroker>().message(&order).publish().await?;

        assert_eq!(
            tb.broker::<LapinBroker>().subscriber("orders").outcomes(),
            [Outcome::Nack, Outcome::Ack],
        );
        tb.broker::<LapinBroker>()
            .subscriber("orders")
            .assert_called(2)
            .with(&order);
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_drop_does_not_come_back() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let order = Order {
            id: 8,
            sku: String::new(),
        };
        tb.broker::<LapinBroker>().message(&order).publish().await?;

        tb.broker::<LapinBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        tb.shutdown().await?;
        Ok(())
    }
}
