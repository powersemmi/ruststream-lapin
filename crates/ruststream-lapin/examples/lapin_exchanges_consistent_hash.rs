//! The consistent-hash exchange of the `rabbitmq_consistent_hash_exchange` plugin: one stream
//! spread over several queues by a hash of the routing key.
//!
//! The server's plugin does the routing, so the in-process transport routes nothing through this
//! exchange; the live test runs against a plugin-enabled broker at `AMQP_PLUGINS_TEST_URL`.
//!
//! ```text
//! cargo test --example lapin_exchanges_consistent_hash --features testing,plugin-consistent-hash
//! AMQP_PLUGINS_TEST_URL=amqp://127.0.0.1:5673 cargo test --example lapin_exchanges_consistent_hash --features testing,plugin-consistent-hash
//! ```

use serde::{Deserialize, Serialize};

use ruststream_lapin::prelude::*;

#[cfg(test)]
#[path = "../tests/live/mod.rs"]
mod live;

#[derive(OutSlot)]
#[publishes(Order)]
struct Sharded;

/// Publishes each order to the exchange `orders-by-customer`, keyed by its customer.
#[subscriber("order-intake")]
async fn shard(order: &Order, Out(sharded): Out<impl Publisher, Sharded>) -> HandlerOutcome {
    if sharded
        .message(order)
        .to(&order.customer)
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

// --8<-- [start:shards]
/// Two shards of equal weight: the binding key is the weight, and one customer's orders always
/// land on the same shard.
#[subscriber(RabbitQueue::new("orders.shard-a")
    .bind(RabbitExchange::consistent_hash("orders-by-customer"), "1"))]
async fn shard_a(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, order.customer, "shard a");
    HandlerOutcome::ack()
}

#[subscriber(RabbitQueue::new("orders.shard-b")
    .bind(RabbitExchange::consistent_hash("orders-by-customer"), "1"))]
async fn shard_b(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, order.customer, "shard b");
    HandlerOutcome::ack()
}
// --8<-- [end:shards]

/// The service against the broker at `uri`.
fn service(uri: impl Into<String>) -> impl App {
    let broker = LapinBroker::new(uri).declare_topology(true);
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(shard)
            .out(Sharded, Publish::default().exchange("orders-by-customer"))
            .build();
        b.include(shard_a);
        b.include(shard_b);
    })
}

#[ruststream::app]
fn app() -> impl App {
    service("amqp://localhost:5672")
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
    customer: String,
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
