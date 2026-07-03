//! A minimal `RabbitMQ` service: one `#[subscriber]` handler on one queue.
//!
//! `LapinBroker::new` is synchronous and does no I/O, so the whole service fits the
//! `#[ruststream::app]` macro. The runtime connects the broker once at startup
//! (`Broker::connect`) before opening subscriptions, and the generated binary understands
//! `run` and `asyncapi gen`.
//!
//! The bare-string subscriber form consumes the queue named `orders` (which must already exist;
//! this crate does not create infrastructure unless the broker opts in, see the
//! `lapin_topology` example). Start a broker and create the queue first:
//!
//! ```text
//! just brokers-up
//! docker exec ruststream-rabbitmq rabbitmqadmin declare queue name=orders durable=true
//! cargo run --example lapin_quickstart -- run
//! ```
//!
//! Publish an order from another terminal:
//!
//! ```text
//! docker exec ruststream-rabbitmq rabbitmqadmin publish routing_key=orders payload='{"id":1}'
//! ```

// --8<-- [start:handler]
use ruststream::runtime::{AppInfo, HandlerResult, RustStream};
use ruststream::subscriber;
use ruststream_lapin::LapinBroker;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

#[subscriber("orders")]
async fn handle(order: &Order) -> HandlerResult {
    println!("got order {}", order.id);
    HandlerResult::Ack
}
// --8<-- [end:handler]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> RustStream {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        LapinBroker::new("amqp://localhost:5672").prefetch(64),
        |b| {
            b.include(handle);
        },
    )
}
// --8<-- [end:app]
