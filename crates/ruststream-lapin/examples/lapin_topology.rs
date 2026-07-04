//! Descriptors and topology: queues, exchange bindings, queue types, and opt-in declaration.
//!
//! Descriptors describe the EXPECTED topology; by default nothing is created on the broker,
//! because managing infrastructure is the user's job. `.declare_topology(true)` is the explicit
//! opt-in: subscribing then declares the bound exchanges, the queue, and the bindings.
//!
//! ```text
//! just brokers-up
//! cargo run --example lapin_topology -- run
//! ```

use std::time::Duration;

use ruststream::runtime::{App, AppInfo, HandlerResult, RustStream};
use ruststream::subscriber;
use serde::Deserialize;

// --8<-- [start:descriptor]
use ruststream_lapin::{AMQPValue, Delay, LapinBroker, QueueType, RabbitExchange, RabbitQueue};

#[derive(Debug, Deserialize)]
struct OrderPlaced {
    id: u64,
}

// One queue, fed by a topic exchange: every `order.*` event lands here. The queue survives
// restarts (durable is the default) and dead-letters rejected messages.
#[subscriber(RabbitQueue::new("orders")
    .queue_type(QueueType::Quorum)
    .bind(RabbitExchange::topic("events"), "order.*")
    .dead_letter_exchange("dead-letters")
    .prefetch(16))]
async fn on_order(event: &OrderPlaced) -> HandlerResult {
    println!("order event {}", event.id);
    HandlerResult::Ack
}
// --8<-- [end:descriptor]

// --8<-- [start:arguments]
// Anything the descriptor does not model rides through verbatim as a raw `x-*` argument.
#[subscriber(RabbitQueue::new("bounded")
    .argument("x-message-ttl", AMQPValue::LongLongInt(60_000))
    .argument("x-max-length", AMQPValue::LongLongInt(100_000)))]
async fn on_bounded(event: &OrderPlaced) -> HandlerResult {
    println!("bounded order {}", event.id);
    HandlerResult::Ack
}
// --8<-- [end:arguments]

// --8<-- [start:delay]
// `.delay(..)` makes `retry_after` native: a delayed message parks in a broker waiting queue for
// the delay, then dead-letters back here - durable, off the service process. The waiting queue
// (`charges.retry` by default) is declared under `declare_topology`.
#[subscriber(RabbitQueue::new("charges").delay(Delay::dlx_ttl()))]
async fn on_charge(event: &OrderPlaced) -> HandlerResult {
    if event.id == 0 {
        // Not ready yet: come back in 30s instead of spinning on an immediate requeue.
        return HandlerResult::retry_after(Duration::from_secs(30));
    }
    HandlerResult::Ack
}
// --8<-- [end:delay]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App {
    // declare_topology is off by default; this service owns its queues, so it opts in.
    // default_queue_type applies to descriptors that do not pick a type themselves.
    let broker = LapinBroker::new("amqp://localhost:5672")
        .declare_topology(true)
        .default_queue_type(QueueType::Quorum)
        .prefetch(64);
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(on_order);
        b.include(on_bounded);
        b.include(on_charge);
    })
}
// --8<-- [end:app]
