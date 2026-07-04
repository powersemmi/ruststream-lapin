//! Server-side sharding with the consistent-hash exchange plugin: one stream spread across
//! several queues by hashing the routing key. Requires the `plugin-consistent-hash` feature and
//! the plugin enabled on the broker.
//!
//! ```text
//! just plugins-up
//! cargo run --example lapin_consistent_hash --features plugin-consistent-hash -- run
//! ```

use ruststream::runtime::{App, AppInfo, HandlerResult, RustStream};
use ruststream::subscriber;
use serde::Deserialize;

// --8<-- [start:shards]
use ruststream_lapin::{LapinBroker, RabbitExchange, RabbitQueue};

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

// Two shards bound to one consistent-hash exchange: the broker splits the hash space by weight
// (the binding key), so orders spread evenly and each key always lands on the same shard.
#[subscriber(RabbitQueue::new("orders-shard-a")
    .bind(RabbitExchange::consistent_hash("orders-by-key"), "1"))]
async fn shard_a(order: &Order) -> HandlerResult {
    println!("shard a: order {}", order.id);
    HandlerResult::Ack
}

#[subscriber(RabbitQueue::new("orders-shard-b")
    .bind(RabbitExchange::consistent_hash("orders-by-key"), "1"))]
async fn shard_b(order: &Order) -> HandlerResult {
    println!("shard b: order {}", order.id);
    HandlerResult::Ack
}
// --8<-- [end:shards]

#[ruststream::app]
fn app() -> impl App {
    let broker = LapinBroker::new("amqp://localhost:5672").declare_topology(true);
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(shard_a);
        b.include(shard_b);
    })
}
