//! The service of `examples/lapin_exchanges_headers.rs` without the macros: the handlers are named types bound to their descriptors, and `main` is
//! written by hand.
//!
//! ```text
//! cargo test --example manual_lapin_exchanges_headers --features testing
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;

use serde::{Deserialize, Serialize};

use ruststream_lapin::prelude::*;

/// The slot routed orders leave through.
struct Routed;

impl OutSlot for Routed {
    const NAME: &'static str = "Routed";
    // The publish call names the routing key, so a transform on this slot may name it too.
    type Destination = Names;
}

impl PublishedThrough<Routed> for Order {}

/// Publishes each order to the headers exchange `orders`, with its region and tier as headers.
struct Route;

impl<Entry> Handle<Order, (), Outs<(Entry,)>> for Route
where
    Entry: OutEntry<Routed, Wire: Publisher>,
{
    async fn handle(
        &self,
        order: &Order,
        outs: &Outs<(Entry,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let mut headers = HeaderMap::new();
        headers.insert("region", order.region.clone());
        headers.insert("tier", order.tier.clone());
        if outs
            .get(Routed)
            .message(order)
            .to("")
            .with_headers(headers)
            .publish()
            .await
            .is_err()
        {
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
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

struct Expedite;

impl Handle<Order> for Expedite {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, "expedited");
        ready(Ok(()))
    }
}

struct Review;

impl Handle<Order> for Review {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, "reviewed");
        ready(Ok(()))
    }
}

/// Gold orders from the EU, where both headers have to match, and orders from the EU or of the
/// gold tier, where one header is enough.
fn bound() -> (RabbitQueue, RabbitQueue) {
    let orders = RabbitExchange::headers("orders");
    let all = RabbitQueue::new("orders.eu-gold").bind_with(orders.clone(), "", gold_in_eu("all"));
    let any = RabbitQueue::new("orders.eu-or-gold").bind_with(orders, "", gold_in_eu("any"));
    (all, any)
}
// --8<-- [end:headers]

fn app() -> impl App {
    let broker = LapinBroker::new("amqp://localhost:5672").declare_topology(true);
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(subscriber("order-intake", Route).build())
            .out(Routed, Publish::default().exchange("orders"))
            .build();
        let (all, any) = bound();
        b.include(subscriber(all, Expedite).build());
        b.include(subscriber(any, Review).build());
    })
}

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
    region: String,
    tier: String,
}

// `#[derive(Outgoing)]` by hand: the publish call names the destination.
impl OutgoingDestination for Order {
    type Form = CallerName;
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
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
