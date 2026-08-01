//! Transactional publishing from a handler: an order fans out into per-item shipment commands,
//! published all-or-nothing through a confirm-transactional publisher the runtime injects into
//! the handler.
//!
//! The publisher is declared as a policy at the mount site (`.publisher(..)`) and arrives in the
//! handler as an `Out` parameter, already live: a handler never sees a publisher without a
//! connection.
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

use ruststream::codec::{Codec, JsonCodec};
use ruststream::runtime::{App, AppInfo, HandlerResult, Out, RustStream};
use ruststream::{OutgoingMessage, Publisher, TransactionalPublisher, subscriber};
use ruststream_lapin::{AmqpError, ConfirmsPublisher, LapinBroker, LapinPublish};
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
async fn dispatch(publisher: &ConfirmsPublisher, order: &Order) -> Result<(), AmqpError> {
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
async fn ship(order: &Order, Out(shipments): Out<ConfirmsPublisher>) -> HandlerResult {
    if dispatch(shipments, order).await.is_err() {
        // Nothing was committed; ask for redelivery and try the whole fan-out again.
        return HandlerResult::retry();
    }
    HandlerResult::Ack
}
// --8<-- [end:handler]

#[ruststream::app]
fn app() -> impl App {
    let broker = LapinBroker::new("amqp://localhost:5672").declare_topology(true);
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        // --8<-- [start:confirms]
        // The transactional flavour is a policy transition; swap `.confirms()` for
        // `.server_tx()` to trade throughput for AMQP server-side atomicity.
        b.include(ship)
            .publisher(LapinPublish::default().confirms());
        // --8<-- [end:confirms]
    })
}
