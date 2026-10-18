//! `RabbitQueue`: a classic queue with its durability, its bindings and its dead-letter route, and
//! a requeue that comes back flagged as redelivered.
//!
//! ```text
//! cargo test --example lapin_subscriptions_rabbit_queue --features testing
//! ```

use serde::{Deserialize, Serialize};

use ruststream_lapin::prelude::*;

// --8<-- [start:descriptor]
/// A durable queue fed by the topic exchange `events`; a dropped order leaves through
/// `orders.failed`. A delivery the server already handed out once is placed on its second try.
#[subscriber(RabbitQueue::new("orders")
    .bind(RabbitExchange::topic("events"), "order.*")
    .dead_letter_exchange("orders.failed"))]
async fn place(order: &Order, Ctx(redelivered): Ctx<Redelivered>) -> HandlerOutcome {
    if !redelivered && order.flaky {
        return HandlerOutcome::retry();
    }
    tracing::info!(order.id, redelivered, "order placed");
    HandlerOutcome::ack()
}

/// A queue of this instance alone: it lives as long as the connection that declared it.
#[subscriber(RabbitQueue::new("prices.instance-a")
    .durable(false)
    .exclusive(true)
    .auto_delete(true)
    .bind(RabbitExchange::fanout("prices"), ""))]
async fn refresh(price: &Price) -> HandlerOutcome {
    tracing::info!(price.sku, price.cents, "price cached");
    HandlerOutcome::ack()
}
// --8<-- [end:descriptor]

#[ruststream::app]
fn app() -> impl App {
    let broker = LapinBroker::new("amqp://localhost:5672").declare_topology(true);
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(place);
        b.include(refresh);
    })
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
    flaky: bool,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "prices.instance-a")]
struct Price {
    sku: String,
    cents: u64,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_lapin::prelude::*;

    use super::{Order, Price, app};

    #[tokio::test]
    async fn a_requeued_order_comes_back_redelivered() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<LapinBroker>()
            .message(&Order { id: 7, flaky: true })
            .publish()
            .await?;

        tb.broker::<LapinBroker>()
            .subscriber("orders")
            .assert_called(2)
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn the_exclusive_queue_takes_a_price() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<LapinBroker>()
            .message(&Price {
                sku: "tea".to_owned(),
                cents: 450,
            })
            .publish()
            .await?;

        tb.broker::<LapinBroker>()
            .subscriber("prices.instance-a")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }
}
