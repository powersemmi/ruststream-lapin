//! Transactional publishing from a handler: an order fans out into per-item shipment commands,
//! published all-or-nothing through a confirm-transactional publisher held in the typed
//! application state (the framework's DI: the handler declares `State<Shipments>` and the
//! runtime injects it).
//!
//! Two `TransactionalPublisher` implementations share the same
//! `begin / publish / commit / abort` surface, picked on the publisher:
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
use ruststream::runtime::{App, AppInfo, HandlerResult, RustStream, State};
use ruststream::{FromRef, OutgoingMessage, Publisher, TransactionalPublisher, subscriber};
use ruststream_lapin::{AmqpError, ConfirmsPublisher, LapinBroker};
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

// --8<-- [start:state]
// The application state wires the use-case object once at startup; `#[derive(FromRef)]` makes
// each field injectable into handlers as `State<FieldType>`.
#[derive(Clone, FromRef)]
struct AppState {
    shipments: Shipments,
}

#[derive(Clone)]
struct Shipments {
    publisher: ConfirmsPublisher,
}

impl Shipments {
    /// Publishes one shipment command per item, all-or-nothing: commit resolves only after the
    /// broker confirmed every message, and any failure aborts so shipments are never
    /// half-visible.
    async fn dispatch(&self, order: &Order) -> Result<(), AmqpError> {
        self.publisher.begin_transaction().await?;
        for item in &order.items {
            let command = ItemShipment {
                order_id: order.id,
                item: item.clone(),
            };
            let payload = JsonCodec.encode(&command).expect("serializable");
            let outgoing = OutgoingMessage::new("shipments", payload.as_ref());
            if let Err(err) = self.publisher.publish(outgoing).await {
                self.publisher.abort().await.ok();
                return Err(err);
            }
        }
        self.publisher.commit().await
    }
}
// --8<-- [end:state]

// --8<-- [start:handler]
#[subscriber("orders")]
async fn ship(order: &Order, State(shipments): State<Shipments>) -> HandlerResult {
    if shipments.dispatch(order).await.is_err() {
        // Nothing was committed; ask for redelivery and try the whole fan-out again.
        return HandlerResult::retry();
    }
    HandlerResult::Ack
}
// --8<-- [end:handler]

#[ruststream::app]
fn app() -> impl App {
    let broker = LapinBroker::new("amqp://localhost:5672").declare_topology(true);
    // --8<-- [start:confirms]
    // The transactional flavour is picked on the publisher; swap `.confirms()` for
    // `.server_tx()` to trade throughput for AMQP server-side atomicity.
    let shipments = Shipments {
        publisher: broker.publisher().confirms(),
    };
    // --8<-- [end:confirms]
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .on_startup(
            move |()| async move { Ok::<_, std::convert::Infallible>(AppState { shipments }) },
        )
        .with_broker(broker, |b| {
            b.include(ship);
        })
}
