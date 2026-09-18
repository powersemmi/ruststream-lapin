//! Per-message AMQP properties from a handler: every shipment carries the priority the mount site
//! fixed, and an expedited one leaves at a higher priority with a TTL of its own.
//!
//! The priority only orders deliveries on a queue declared with `x-max-priority`:
//!
//! ```text
//! just brokers-up
//! docker exec ruststream-rabbitmq rabbitmqadmin declare queue --name orders --durable true
//! docker exec ruststream-rabbitmq rabbitmqadmin declare queue --name shipments --durable true \
//!     'arguments={"x-max-priority":10}'
//! cargo run --example lapin_priority -- run
//! ```

use std::time::Duration;

use ruststream_lapin::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
    expedited: bool,
}

#[derive(Debug, Outgoing, Serialize)]
#[outgoing(name = "shipments")]
struct Shipment {
    order_id: u64,
}

// --8<-- [start:steps]
/// An expedited order jumps the queue: `priority` writes the AMQP `priority` property for this one
/// message, and `expiration` gives it an hour before the broker drops it. An ordinary order names
/// neither step and ships at the priority the mount site fixed.
///
/// The slot is bounded by the options type rather than by a publisher type, so the body still
/// names no broker type; the steps come from this crate's prelude.
#[subscriber("orders")]
async fn ship(
    order: &Order,
    Out(shipments): Out<impl Publisher<Options = LapinPublishOptions>>,
) -> HandlerOutcome {
    let shipment = Shipment { order_id: order.id };
    let sent = if order.expedited {
        shipments
            .message(&shipment)
            .priority(9)
            .expiration(Duration::from_secs(3600))
            .publish()
            .await
    } else {
        shipments.message(&shipment).publish().await
    };

    if sent.is_err() {
        // Nothing was handed to the broker; ask for redelivery and ship it again.
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}
// --8<-- [end:steps]

// --8<-- [start:mount]
#[ruststream::app]
fn app() -> impl App {
    let broker = LapinBroker::new("amqp://localhost:5672");
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        // Every shipment leaves at priority 3 unless the handler says otherwise.
        b.include(ship)
            .out(DefaultSlot, Publish::default().priority(3))
            .build();
    })
}
// --8<-- [end:mount]
