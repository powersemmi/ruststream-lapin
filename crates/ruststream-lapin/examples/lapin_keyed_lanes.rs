//! Keyed worker lanes: several lanes process one queue in parallel, but deliveries that share a
//! partition key stay on the same lane (ordered per key). The key rides in the
//! `PARTITION_KEY_HEADER`, which the crate reads back through the `Partitioned` capability.
//!
//! ```text
//! just brokers-up
//! cargo run --example lapin_keyed_lanes -- run
//! ```

use ruststream::runtime::{App, AppInfo, HandlerResult, RustStream};
use ruststream::subscriber;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
    tenant: String,
}

// --8<-- [start:consumer]
// Eight lanes, keyed: two orders for the same tenant never process concurrently (per-tenant
// ordering), while different tenants run in parallel. `by_key` reads the partition key the
// producer set on the message.
#[subscriber(RabbitQueue::new("orders"), workers(8, by_key))]
async fn on_order(order: &Order) -> HandlerResult {
    println!("order {} for tenant {}", order.id, order.tenant);
    HandlerResult::Ack
}
// --8<-- [end:consumer]

use ruststream_lapin::{LapinBroker, RabbitQueue};

#[ruststream::app]
fn app() -> impl App {
    let broker = LapinBroker::new("amqp://localhost:5672").declare_topology(true);
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(on_order);
    })
}
