//! A fanout exchange: every bound queue gets a copy of every message, whatever its routing key.
//!
//! ```text
//! cargo test --example lapin_exchanges_fanout --features testing
//! ```

use serde::{Deserialize, Serialize};

use ruststream_lapin::prelude::*;

#[derive(OutSlot)]
#[publishes(PriceChanged)]
struct Prices;

/// Publishes every price change to the exchange `prices`.
#[subscriber("price-updates")]
async fn publish_price(
    update: &PriceUpdate,
    Out(prices): Out<impl Publisher, Prices>,
) -> HandlerOutcome {
    let changed = PriceChanged {
        sku: update.sku.clone(),
        cents: update.cents,
    };
    if prices
        .message(&changed)
        .to(&update.sku)
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

// --8<-- [start:fanout]
#[subscriber(RabbitQueue::new("prices.search").bind(RabbitExchange::fanout("prices"), ""))]
async fn reindex(price: &PriceChanged) -> HandlerOutcome {
    tracing::info!(price.sku, "search index updated");
    HandlerOutcome::ack()
}

#[subscriber(RabbitQueue::new("prices.cache").bind(RabbitExchange::fanout("prices"), ""))]
async fn recache(price: &PriceChanged) -> HandlerOutcome {
    tracing::info!(price.sku, price.cents, "cache updated");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    let broker = LapinBroker::new("amqp://localhost:5672").declare_topology(true);
    RustStream::new(AppInfo::new("catalog", "0.1.0")).with_broker(broker, |b| {
        b.include(publish_price)
            .out(Prices, Publish::default().exchange("prices"))
            .build();
        b.include(reindex);
        b.include(recache);
    })
}
// --8<-- [end:fanout]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "price-updates")]
struct PriceUpdate {
    sku: String,
    cents: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct PriceChanged {
    sku: String,
    cents: u64,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_lapin::prelude::*;

    use super::{PriceChanged, PriceUpdate, app};

    #[tokio::test]
    async fn every_bound_queue_gets_the_price() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<LapinBroker>()
            .message(&PriceUpdate {
                sku: "tea".to_owned(),
                cents: 450,
            })
            .publish()
            .await?;

        let expected = PriceChanged {
            sku: "tea".to_owned(),
            cents: 450,
        };
        tb.broker::<LapinBroker>()
            .subscriber("prices.search")
            .assert_called_once()
            .with(&expected);
        tb.broker::<LapinBroker>()
            .subscriber("prices.cache")
            .assert_called_once()
            .with(&expected);
        tb.shutdown().await?;
        Ok(())
    }
}
