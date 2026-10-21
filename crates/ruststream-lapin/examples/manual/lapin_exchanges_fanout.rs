//! The service of `examples/lapin_exchanges_fanout.rs` without the macros: the handlers are named types bound to their descriptors, and `main` is
//! written by hand.
//!
//! ```text
//! cargo test --example manual_lapin_exchanges_fanout --features testing
//! ```

use std::future::ready;
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;

use serde::{Deserialize, Serialize};

use ruststream_lapin::prelude::*;

/// The slot price changes leave through.
struct Prices;

impl OutSlot for Prices {
    const NAME: &'static str = "Prices";
    // The publish call names the routing key, so a transform on this slot may name it too.
    type Destination = Names;
}

impl PublishedThrough<Prices> for PriceChanged {}

/// Publishes every price change to the exchange `prices`.
struct PublishPrice;

impl<Entry> Handle<PriceUpdate, (), Outs<(Entry,)>> for PublishPrice
where
    Entry: OutEntry<Prices, Wire: Publisher>,
{
    async fn handle(
        &self,
        update: &PriceUpdate,
        outs: &Outs<(Entry,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let changed = PriceChanged {
            sku: update.sku.clone(),
            cents: update.cents,
        };
        if outs
            .get(Prices)
            .message(&changed)
            .to(&update.sku)
            .publish()
            .await
            .is_err()
        {
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
}

// --8<-- [start:fanout]
struct Reindex;

impl Handle<PriceChanged> for Reindex {
    fn handle(
        &self,
        price: &PriceChanged,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(price.sku, "search index updated");
        ready(Ok(()))
    }
}

struct Recache;

impl Handle<PriceChanged> for Recache {
    fn handle(
        &self,
        price: &PriceChanged,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(price.sku, price.cents, "cache updated");
        ready(Ok(()))
    }
}

fn app() -> impl App {
    let broker = LapinBroker::new("amqp://localhost:5672").declare_topology(true);
    RustStream::new(AppInfo::new("catalog", "0.1.0")).with_broker(broker, |b| {
        let prices = RabbitExchange::fanout("prices");
        b.include(subscriber("price-updates", PublishPrice).build())
            .out(Prices, Publish::default().exchange("prices"))
            .build();
        let search = RabbitQueue::new("prices.search").bind(prices.clone(), "");
        b.include(subscriber(search, Reindex).build());
        let cache = RabbitQueue::new("prices.cache").bind(prices, "");
        b.include(subscriber(cache, Recache).build());
    })
}
// --8<-- [end:fanout]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct PriceUpdate {
    sku: String,
    cents: u64,
}

// `#[outgoing(name = "price-updates")]` by hand: the type names the queue it is published to.
impl OutgoingDestination for PriceUpdate {
    type Form = FixedName;
    const DESTINATION: &'static str = "price-updates";
}

impl MessageHeaders for PriceUpdate {
    type Contract = NoHeaders;
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct PriceChanged {
    sku: String,
    cents: u64,
}

// `#[derive(Outgoing)]` by hand: the publish call names the destination.
impl OutgoingDestination for PriceChanged {
    type Form = CallerName;
}

impl MessageHeaders for PriceChanged {
    type Contract = NoHeaders;
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
