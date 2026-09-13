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

// --8<-- [start:batches]
// A batch handler: the slice parameter is what asks for one. AMQP pushes one delivery at a time,
// so the crate assembles the batch on the client - hence the two descriptor options: the prefetch
// window has to be at least as wide as the batch size or the broker never has enough in flight to
// fill one, and `batch_wait` caps how long a batch that never fills keeps its deliveries.
#[subscriber(RabbitQueue::new("settlements")
    .prefetch(nonzero!(64))
    .batch_wait(Duration::from_millis(200)))]
async fn on_settlement(events: &[OrderPlaced]) -> HandlerOutcome {
    println!("settling {} orders", events.len());
    HandlerOutcome::ack()
}
// --8<-- [end:batches]

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

// --8<-- [start:retry_fallback]
// Without `.delay(..)` the delayed copy is the runtime's to publish, and it goes back under this
// queue's own name: on the default exchange a routing key addresses the queue that carries it.
#[subscriber(RabbitQueue::new("refunds"))]
async fn on_refund(event: &OrderPlaced) -> HandlerOutcome {
    if event.id == 0 {
        return HandlerOutcome::retry_after(Duration::from_secs(30));
    }
    HandlerOutcome::ack()
}
// --8<-- [end:retry_fallback]

// --8<-- [start:capped]
// A message that is never ready circulates until an operator steps in. A quorum queue counts the
// deliveries a message spends, so the cap declared at the mount site becomes the queue's own
// delivery limit and the destination its dead-letter route: a spent delivery leaves even while no
// service is running.
#[subscriber(RabbitQuorumQueue::new("payouts"))]
async fn on_payout(event: &OrderPlaced) -> HandlerOutcome {
    if event.id == 0 {
        return HandlerOutcome::retry_after(Duration::from_secs(30));
    }
    HandlerOutcome::ack()
}
// --8<-- [end:capped]

// --8<-- [start:quorum_own_policy]
// A quorum queue whose retry policy is the queue's own rather than one registration's: the limit
// and the route are descriptor settings, and the mount site declares nothing.
#[subscriber(RabbitQuorumQueue::new("reservations")
    .delivery_limit(4)
    .dead_letter_exchange("dead-letters"))]
async fn on_reservation(event: &OrderPlaced) -> HandlerOutcome {
    if event.id == 0 {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}
// --8<-- [end:quorum_own_policy]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App {
    // declare_topology is off by default; this service owns its queues, so it opts in. That is
    // also what lets a mount-site cap reach a quorum queue, which carries it as its own arguments.
    let broker = LapinBroker::new("amqp://localhost:5672")
        .declare_topology(true)
        .prefetch(nonzero!(64));
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(on_order);
        b.include(on_bounded);
        b.include(on_charge);
        b.include(on_reservation);
        // --8<-- [start:retry_mount]
        // Every registration already has a publisher for its copies, taken from the broker's
        // default policy. Naming one replaces it: the confirms publisher waits for the broker to
        // own the copy, so a retry is not lost by a fire-and-forget publish.
        b.include(on_refund)
            .out_retry(TransactionalPublish::default());
        // --8<-- [end:retry_mount]
        // --8<-- [start:declaration]
        // How many deliveries one message gets, counting the first, and where it goes once they
        // run out. Both steps follow `include` and read the same on every broker; on a quorum
        // queue this service declares they become `x-delivery-limit` and the dead-letter route.
        b.include(on_payout)
            .max_attempts(nonzero!(5u32))
            .dead_letter("payouts.dead");
        // --8<-- [end:declaration]
        // --8<-- [start:batches_mount]
        // The batch size is the mount site's word, and a batch handler does not mount without it.
        b.include(on_settlement.batch(nonzero!(32)));
        // --8<-- [end:batches_mount]
    })
}
// --8<-- [end:app]
