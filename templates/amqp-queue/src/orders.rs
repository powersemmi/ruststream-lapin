//! Domain types and handlers, written as `#[subscriber]` functions.
//!
//! The first parameter is the decoded payload; the macro turns each function into a mountable
//! definition (a value named after the function) that `routes` collects into a `Router`. The
//! bare-string subscriber form consumes the queue with that name off the default exchange, so the
//! routing key is the queue name and consumers on the same queue compete for deliveries. Each
//! delivery is `basic.ack`ed when the handler returns `Ack`.

use ruststream::runtime::HandlerResult;
use ruststream::subscriber;
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
#[derive(Debug, Serialize, JsonSchema)]
pub struct Confirmation {
    pub id: u64,
    pub accepted: bool,
}

/// Confirms an incoming order and publishes a `Confirmation` to the `confirmations` queue.
///
/// The `publish("confirmations")` clause makes the runtime encode the return value and publish it
/// through the publisher wired in `routes` (the default exchange routes it to the queue named
/// `confirmations`).
#[subscriber("orders", publish("confirmations"))]
pub async fn confirm(order: &Order) -> Confirmation {
    Confirmation {
        id: order.id,
        accepted: order.quantity > 0,
    }
}

/// Logs cancellations from the `cancellations` queue. No reply, so it returns a plain
/// `HandlerResult`; `Ack` triggers the `basic.ack`.
#[subscriber("cancellations")]
pub async fn on_cancel(order: &Order) -> HandlerResult {
    println!("order {} ({}) cancelled", order.id, order.item);
    HandlerResult::Ack
}
