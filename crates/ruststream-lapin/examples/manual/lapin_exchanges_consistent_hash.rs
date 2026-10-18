//! The service of `examples/lapin_exchanges_consistent_hash.rs` without the macros: the handlers are named types bound to their descriptors, and `main` is
//! written by hand.
//!
//! ```text
//! cargo test --example manual_lapin_exchanges_consistent_hash --features testing
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;

use serde::{Deserialize, Serialize};

#[cfg(test)]
#[path = "../../tests/live/mod.rs"]
mod live;

use ruststream_lapin::prelude::*;

/// The slot sharded orders leave through.
struct Sharded;

impl OutSlot for Sharded {
    const NAME: &'static str = "Sharded";
    // The publish call names the routing key, so a transform on this slot may name it too.
    type Destination = Names;
}

impl PublishedThrough<Sharded> for Order {}

/// Publishes each order to the exchange `orders-by-customer`, keyed by its customer.
struct Shard;

impl<Entry> Handle<Order, (), Outs<(Entry,)>> for Shard
where
    Entry: OutEntry<Sharded, Wire: Publisher>,
{
    async fn handle(
        &self,
        order: &Order,
        outs: &Outs<(Entry,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        if outs
            .get(Sharded)
            .message(order)
            .to(&order.customer)
            .publish()
            .await
            .is_err()
        {
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
}

// --8<-- [start:shards]
struct ShardA;

impl Handle<Order> for ShardA {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, order.customer, "shard a");
        ready(Ok(()))
    }
}

struct ShardB;

impl Handle<Order> for ShardB {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, order.customer, "shard b");
        ready(Ok(()))
    }
}

/// Two shards of equal weight: the binding key is the weight, and one customer's orders always
/// land on the same shard.
fn shards() -> (RabbitQueue, RabbitQueue) {
    let by_customer = RabbitExchange::consistent_hash("orders-by-customer");
    let a = RabbitQueue::new("orders.shard-a").bind(by_customer.clone(), "1");
    let b = RabbitQueue::new("orders.shard-b").bind(by_customer, "1");
    (a, b)
}
// --8<-- [end:shards]

/// The service against the broker at `uri`.
fn service(uri: impl Into<String>) -> impl App {
    let broker = LapinBroker::new(uri).declare_topology(true);
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(subscriber("order-intake", Shard).build())
            .out(Sharded, Publish::default().exchange("orders-by-customer"))
            .build();
        let (a, shard_b) = shards();
        b.include(subscriber(a, ShardA).build());
        b.include(subscriber(shard_b, ShardB).build());
    })
}

fn app() -> impl App {
    service("amqp://localhost:5672")
}

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
    customer: String,
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

    use super::{Order, app, live, service};

    fn order(id: u64) -> Order {
        Order {
            id,
            customer: format!("customer-{}", id % 10),
        }
    }

    // In process the service declares the plugin exchange and its bindings, and the exchange
    // routes nothing: the routing is the plugin's.
    #[tokio::test]
    async fn in_process_the_plugin_exchange_routes_nothing() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<LapinBroker>()
            .message(&order(1))
            .to("order-intake")
            .publish()
            .await?;

        tb.broker::<LapinBroker>()
            .subscriber("order-intake")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.broker::<LapinBroker>()
            .subscriber("orders.shard-a")
            .assert_not_called();
        tb.broker::<LapinBroker>()
            .subscriber("orders.shard-b")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }

    // Against the plugin the twenty orders spread over both shards, and each customer's orders
    // stay on one shard.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_plugin_spreads_customers_over_the_shards() -> Result<(), Box<dyn Error>> {
        let Some(uri) = live::url("AMQP_PLUGINS_TEST_URL") else {
            return Ok(());
        };
        let tb = TestApp::start_live(service(uri)).await?;
        for id in 0..20 {
            tb.broker::<LapinBroker>()
                .message(&order(id))
                .to("order-intake")
                .publish()
                .await?;
        }

        let a = tb
            .broker::<LapinBroker>()
            .subscriber("orders.shard-a")
            .received::<Order>();
        let b = tb
            .broker::<LapinBroker>()
            .subscriber("orders.shard-b")
            .received::<Order>();
        assert_eq!(a.len() + b.len(), 20, "{a:?} {b:?}");
        assert!(!a.is_empty() && !b.is_empty(), "{a:?} {b:?}");
        assert!(
            a.iter()
                .all(|order| b.iter().all(|other| other.customer != order.customer)),
            "a customer split over both shards: {a:?} {b:?}"
        );
        tb.shutdown().await?;
        Ok(())
    }
}
