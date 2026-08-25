//! Per-message AMQP properties from a handler: an expedited order leaves at a higher priority
//! and with a TTL, through the publish steps this crate puts in front of the core publish
//! builder.
//!
//! The handler names the capability it needs (`Out<impl LapinPublishExt>`) and the concrete
//! publisher comes from the policy attached at the mount site, so a step is taken on a publisher
//! that is already live.
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

use ruststream::runtime::{App, AppInfo, HandlerResult, Out, PublishExt, RustStream};
use ruststream::{Outgoing, subscriber};
use ruststream_lapin::{LapinBroker, LapinPublish, LapinPublishExt};
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
/// An expedited order jumps the queue: `with_priority` writes the AMQP `priority` property, which
/// orders it ahead of the ordinary shipments, and `with_expiration` gives it an hour to be picked
/// up before the broker drops it. Written as plain headers, neither value would reach RabbitMQ.
#[subscriber("orders")]
async fn ship(order: &Order, Out(shipments): Out<impl LapinPublishExt>) -> HandlerResult {
    let shipment = Shipment { order_id: order.id };
    let sent = if order.expedited {
        shipments
            .with_priority(9)
            .with_expiration(Duration::from_secs(3600))
            .message(&shipment)
            .publish()
            .await
    } else {
        shipments.message(&shipment).publish().await
    };

    if sent.is_err() {
        // Nothing was handed to the broker; ask for redelivery and ship it again.
        return HandlerResult::retry();
    }
    HandlerResult::Ack
}
// --8<-- [end:steps]

#[ruststream::app]
fn app() -> impl App {
    let broker = LapinBroker::new("amqp://localhost:5672");
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(ship).publisher(LapinPublish::default());
    })
}
