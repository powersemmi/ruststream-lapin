//! A headers exchange: `bind_with` binds a queue by the headers a message has to carry, all of
//! them or any one.
//!
//! ```text
//! cargo test --example lapin_exchanges_headers --features testing
//! ```

use serde::{Deserialize, Serialize};

use ruststream_lapin::prelude::*;

#[derive(OutSlot)]
#[publishes(Order)]
struct Routed;

/// Publishes each order to the headers exchange `orders`, with its region and tier as headers.
#[subscriber("order-intake")]
async fn route(order: &Order, Out(routed): Out<impl Publisher, Routed>) -> HandlerOutcome {
    let mut headers = HeaderMap::new();
    headers.insert("region", order.region.clone());
    headers.insert("tier", order.tier.clone());
    if routed
        .message(order)
        .to("")
        .with_headers(headers)
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

// --8<-- [start:headers]
/// The binding arguments: how to match, then the headers matched on.
fn gold_in_eu(x_match: &str) -> FieldTable {
    let mut arguments = FieldTable::default();
    arguments.insert("x-match".into(), AMQPValue::LongString(x_match.into()));
    arguments.insert("region".into(), AMQPValue::LongString("eu".into()));
    arguments.insert("tier".into(), AMQPValue::LongString("gold".into()));
    arguments
}

/// Gold orders from the EU: both headers have to match.
#[subscriber(RabbitQueue::new("orders.eu-gold")
    .bind_with(RabbitExchange::headers("orders"), "", gold_in_eu("all")))]
async fn expedite(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "expedited");
    HandlerOutcome::ack()
}

/// Orders from the EU or of the gold tier: one header is enough.
#[subscriber(RabbitQueue::new("orders.eu-or-gold")
    .bind_with(RabbitExchange::headers("orders"), "", gold_in_eu("any")))]
async fn review(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "reviewed");
    HandlerOutcome::ack()
}
// --8<-- [end:headers]

#[ruststream::app]
fn app() -> impl App {
    let broker = LapinBroker::new("amqp://localhost:5672").declare_topology(true);
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(route)
            .out(Routed, Publish::default().exchange("orders"))
            .build();
        b.include(expedite);
        b.include(review);
    })
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
    region: String,
    tier: String,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_lapin::prelude::*;

    use super::{Order, app};

    fn order(id: u64, region: &str, tier: &str) -> Order {
        Order {
            id,
            region: region.to_owned(),
            tier: tier.to_owned(),
        }
    }

    #[tokio::test]
    async fn the_headers_decide_the_queues() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let orders = [
            order(1, "eu", "gold"),
            order(2, "eu", "silver"),
            order(3, "us", "gold"),
            order(4, "us", "silver"),
        ];
        for order in &orders {
            tb.broker::<LapinBroker>()
                .message(order)
                .to("order-intake")
                .publish()
                .await?;
        }

        let all: Vec<u64> = tb
            .broker::<LapinBroker>()
            .subscriber("orders.eu-gold")
            .received::<Order>()
            .into_iter()
            .map(|order| order.id)
            .collect();
        let any: Vec<u64> = tb
            .broker::<LapinBroker>()
            .subscriber("orders.eu-or-gold")
            .received::<Order>()
            .into_iter()
            .map(|order| order.id)
            .collect();
        assert_eq!(all, [1]);
        assert_eq!(any, [1, 2, 3]);
        tb.shutdown().await?;
        Ok(())
    }
}
