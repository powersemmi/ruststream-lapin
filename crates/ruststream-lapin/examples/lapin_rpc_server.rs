//! The responder half of RPC over `RabbitMQ` direct reply-to: an inventory service that other
//! services query synchronously through the broker (its counterpart is the
//! `lapin_rpc_client` example).
//!
//! The responder is an ordinary `#[subscriber(.., publish(..))]` handler; what makes it an RPC
//! responder is one static `PublishTransform` that redirects each reply to the private address
//! the requester stamped on the request. The handler itself knows nothing about reply-to.
//!
//! ```text
//! just brokers-up
//! cargo run --example lapin_rpc_server -- run
//! ```
//!
//! Then start the client service from another terminal (see `lapin_rpc_client`).

use ruststream::runtime::{
    App, AppInfo, HandlerResult, Outgoing, PublishContext, PublishTransform, RustStream,
    TypedPublisher,
};
use ruststream::subscriber;
use ruststream_lapin::LapinBroker;
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

// --8<-- [start:transform]
/// The responder half of the direct reply-to convention, as a static publish transform:
/// redirect the reply to the requester's private address and echo its correlation id. Requests
/// without a reply-to fall through to the mount's static destination.
struct DirectReplyTo;

impl<C> PublishTransform<C> for DirectReplyTo {
    fn apply(&self, out: &mut Outgoing<'_>, cx: &PublishContext<'_, C>) {
        if let Some(reply_to) = cx.headers().reply_to() {
            out.set_name(reply_to.to_owned());
        }
        if let Some(correlation_id) = cx.headers().correlation_id() {
            out.headers_mut()
                .insert("correlation-id", correlation_id.as_bytes().to_vec());
        }
    }
}
// --8<-- [end:transform]

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
