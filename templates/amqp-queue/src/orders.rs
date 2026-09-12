//! Domain types and handlers, written as `#[subscriber]` functions.
//!
//! The first parameter is the decoded payload; the macro turns each function into a mountable
//! definition (a value named after the function) that `routes` collects into a `Router`. The
//! bare-string subscriber form consumes the queue with that name off the default exchange, so the
//! routing key is the queue name and consumers on the same queue compete for deliveries. Each
//! delivery is `basic.ack`ed when the handler returns an acking outcome.

use ruststream_lapin::prelude::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// An order placed on the `orders` queue.
///
/// `JsonSchema` lets `asyncapi gen` emit this payload's schema into the generated document.
#[derive(Debug, Deserialize, JsonSchema)]
pub struct Order {
    pub id: u64,
    pub item: String,
    pub quantity: u32,
}

/// The reply published to the `confirmations` queue for each order.
///
/// `Outgoing` declares the destination: the default exchange routes that name to the queue
/// called `confirmations`.
#[derive(Debug, Serialize, JsonSchema, Outgoing)]
#[outgoing(name = "confirmations")]
pub struct Confirmation {
    pub id: u64,
    pub accepted: bool,
}

/// Confirms an incoming order and publishes a `Confirmation` to the `confirmations` queue.
///
/// The `publish` clause makes the runtime encode the return value and publish it through the
/// publisher wired in `routes`, at the destination `Confirmation` declares.
#[subscriber("orders", publish)]
pub async fn confirm(order: &Order) -> Confirmation {
    Confirmation {
        id: order.id,
        accepted: order.quantity > 0,
    }
}

/// Logs cancellations from the `cancellations` queue. No reply, so it returns a plain
/// `HandlerOutcome`; `ack()` triggers the `basic.ack`.
#[subscriber("cancellations")]
pub async fn on_cancel(order: &Order) -> HandlerOutcome {
    println!("order {} ({}) cancelled", order.id, order.item);
    HandlerOutcome::ack()
}
