//! Transactional publishing from a handler: an order fans out into per-item shipment commands,
//! published all-or-nothing through the transactional publisher the runtime injects into the
//! handler.
//!
//! The handler names the capability it needs (`Out<impl TransactionalPublisher>`); the concrete
//! publisher comes from the policy the mount site binds to the slot (`.out(marker, policy)`) and
//! arrives already live, so a handler never sees a publisher without a connection.
//!
//! Two `TransactionalPublisher` implementations share the same
//! `begin / publish / commit / abort` surface, picked on the policy:
//!
//! - `.confirms()` buffers client-side and awaits every broker confirm on commit: durable and
//!   fast, the recommended default.
//! - `.server_tx()` uses AMQP channel transactions (`tx.select`): atomic visibility at commit,
//!   at the cost of a synchronous commit round trip.
//!
//! ```text
//! just brokers-up
//! cargo run --example lapin_transactions -- run
//! ```

use ruststream::OutgoingMessage;
use ruststream::codec::{Codec, JsonCodec};
use ruststream_lapin::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
    items: Vec<String>,
}

#[derive(Debug, Serialize)]
struct ItemShipment {
    order_id: u64,
    item: String,
}

// --8<-- [start:dispatch]
/// Publishes one shipment command per item, all-or-nothing: commit resolves only after the
/// broker confirmed every message, and any failure aborts so shipments are never half-visible.
async fn dispatch<P>(publisher: &P, order: &Order) -> Result<(), P::Error>
where
    P: TransactionalPublisher,
{
    publisher.begin_transaction().await?;
    for item in &order.items {
        let command = ItemShipment {
            order_id: order.id,
            item: item.clone(),
        };
        let payload = JsonCodec.encode(&command).expect("serializable");
        let outgoing = OutgoingMessage::new("shipments", payload.as_ref());
        if let Err(err) = publisher.publish(outgoing).await {
            publisher.abort().await.ok();
            return Err(err);
        }
    }
    publisher.commit().await
}
// --8<-- [end:dispatch]

// --8<-- [start:handler]
#[subscriber("orders")]
async fn ship(order: &Order, Out(shipments): Out<impl TransactionalPublisher>) -> HandlerOutcome {
    if dispatch(shipments, order).await.is_err() {
        // Nothing was committed; ask for redelivery and try the whole fan-out again.
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}
// --8<-- [end:handler]

#[ruststream::app]
fn app() -> impl App {
    let broker = LapinBroker::new("amqp://localhost:5672").declare_topology(true);
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        // --8<-- [start:confirms]
        // `TransactionalPublish` is the confirms policy, the uniform mount-site name every
        // broker in the family answers to. For AMQP server-side atomicity instead, name the
        // other transition: `LapinPublish::default().server_tx()`.
        b.include(ship)
            .out(DefaultSlot, TransactionalPublish::default())
            .build();
        // --8<-- [end:confirms]
    })
}
