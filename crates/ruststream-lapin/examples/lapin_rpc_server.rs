//! The responder half of RPC over `RabbitMQ` direct reply-to: an inventory service that other
//! services query synchronously through the broker (its counterpart is the
//! `lapin_rpc_client` example).
//!
//! The responder is an ordinary `#[subscriber(.., publish(..))]` handler; what makes it an RPC
//! responder is the crate's [`DirectReplyTo`] transform composed onto the reply publisher, which
//! redirects each reply to the private address the requester stamped on the request. The
//! handler itself knows nothing about reply-to.
//!
//! ```text
//! just brokers-up
//! cargo run --example lapin_rpc_server -- run
//! ```
//!
//! Then start the client service from another terminal (see `lapin_rpc_client`).

use ruststream::runtime::{App, AppInfo, HandlerResult, RustStream, TypedPublisher};
use ruststream::subscriber;
use ruststream_lapin::{DirectReplyTo, LapinBroker};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
struct CheckStock {
    sku: String,
    quantity: u32,
}

#[derive(Debug, Serialize)]
struct Stock {
    sku: String,
    available: bool,
}

// --8<-- [start:handler]
// A plain publishing handler: decode the request, return the reply. `Err` settles without
// replying, and the requester's timeout is the recovery mechanism.
#[subscriber("inventory.check", publish("inventory.check.unrouted"))]
async fn check(req: &CheckStock) -> Result<Stock, HandlerResult> {
    if req.sku.is_empty() {
        return Err(HandlerResult::drop());
    }
    // Stand-in for a warehouse lookup.
    Ok(Stock {
        sku: req.sku.clone(),
        available: req.quantity <= 10,
    })
}
// --8<-- [end:handler]

#[ruststream::app]
fn app() -> impl App {
    let broker = LapinBroker::new("amqp://localhost:5672").declare_topology(true);
    // --8<-- [start:mount]
    RustStream::new(AppInfo::new("inventory", "0.1.0")).with_broker(broker, |b| {
        let replies = TypedPublisher::new(b.broker().publisher()).transform(DirectReplyTo);
        b.include_publishing(check, replies);
    })
    // --8<-- [end:mount]
}
