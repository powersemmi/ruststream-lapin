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

use ruststream::nonzero;
use serde::Deserialize;

// --8<-- [start:descriptor]
use ruststream_lapin::prelude::*;

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
    .prefetch(nonzero!(16)))]
async fn on_order(event: &OrderPlaced) -> HandlerOutcome {
    println!("order event {}", event.id);
    HandlerOutcome::ack()
}
// --8<-- [end:descriptor]

// --8<-- [start:arguments]
// Anything the descriptor does not model rides through verbatim as a raw `x-*` argument.
#[subscriber(RabbitQueue::new("bounded")
    .argument("x-message-ttl", AMQPValue::LongLongInt(60_000))
    .argument("x-max-length", AMQPValue::LongLongInt(100_000)))]
async fn on_bounded(event: &OrderPlaced) -> HandlerOutcome {
    println!("bounded order {}", event.id);
    HandlerOutcome::ack()
}
// --8<-- [end:arguments]

// --8<-- [start:pages]
// A page handler: the slice parameter is what asks for one. AMQP pushes one delivery at a time,
// so the crate assembles the page on the client - hence the two descriptor options: the prefetch
// window has to be at least as wide as the page size or the broker never has enough in flight to
// fill one, and `page_wait` caps how long a page that never fills keeps its deliveries.
#[subscriber(RabbitQueue::new("settlements")
    .prefetch(nonzero!(64))
    .page_wait(Duration::from_millis(200)))]
async fn on_settlement(events: &[OrderPlaced]) -> HandlerOutcome {
    println!("settling {} orders", events.len());
    HandlerOutcome::ack()
}
// --8<-- [end:pages]

// --8<-- [start:delay]
// `.delay(..)` makes `retry_after` native: a delayed message parks in a broker waiting queue for
// the delay, then dead-letters back here - durable, off the service process. The waiting queue
// (`charges.retry` by default) is declared under `declare_topology`.
#[subscriber(RabbitQueue::new("charges").delay(Delay::dlx_ttl()))]
async fn on_charge(event: &OrderPlaced) -> HandlerOutcome {
    if event.id == 0 {
        // Not ready yet: come back in 30s instead of spinning on an immediate requeue.
        return HandlerOutcome::retry_after(Duration::from_secs(30));
    }
    HandlerOutcome::ack()
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
        .prefetch(nonzero!(64));
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(on_order);
        b.include(on_bounded);
        b.include(on_charge);
        // --8<-- [start:pages_mount]
        // The page size is the mount site's word, and a page handler does not mount without it.
        b.include(on_settlement.batch(nonzero!(32)));
        // --8<-- [end:pages_mount]
    })
}
// --8<-- [end:app]
