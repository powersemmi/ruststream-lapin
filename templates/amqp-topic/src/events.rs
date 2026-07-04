//! Domain types and handlers, written as `#[subscriber]` functions.
//!
//! The handler binds to a [`RabbitQueue`] descriptor bound to the `events` topic exchange under
//! the `order.*` pattern, so every routing key matching `order.<something>` lands here. Under
//! `declare_topology(true)` the exchange, queue, and binding are declared when the subscription
//! opens. Each delivery is `basic.ack`ed when the handler returns `Ack`.

use ruststream::runtime::HandlerResult;
use ruststream::subscriber;
use ruststream_lapin::{RabbitExchange, RabbitQueue};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// An order event published to the `events` exchange (for example with routing key `order.placed`).
#[derive(Debug, Deserialize, JsonSchema)]
pub struct OrderEvent {
    pub id: u64,
    pub kind: String,
}

/// The reply published back to the `events` exchange under the `order.recorded` routing key.
#[derive(Debug, Serialize, JsonSchema)]
pub struct Recorded {
    pub id: u64,
}

/// Records every `order.*` event and replies with a `Recorded` acknowledgement.
///
/// The queue is bound to the `events` topic exchange under `order.*`; the `publish("order.recorded")`
/// clause makes the runtime encode the return value and publish it (the router wires a publisher
/// targeting the `events` exchange).
#[subscriber(
    RabbitQueue::new("order-events").bind(RabbitExchange::topic("events"), "order.*"),
    publish("order.recorded")
)]
pub async fn record(event: &OrderEvent) -> Recorded {
    println!("recording order {} ({})", event.id, event.kind);
    Recorded { id: event.id }
}

/// Logs shipment events bound under a different pattern on the same exchange. No reply.
#[subscriber(RabbitQueue::new("shipment-events").bind(RabbitExchange::topic("events"), "shipment.*"))]
pub async fn on_shipment(event: &OrderEvent) -> HandlerResult {
    println!("shipment event {} ({})", event.id, event.kind);
    HandlerResult::Ack
}
