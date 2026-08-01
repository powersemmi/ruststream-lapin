//! The requester half of RPC over `RabbitMQ` direct reply-to: an order service that
//! synchronously checks stock in the inventory service before accepting an order - a
//! service-to-service call through the broker, with no HTTP sidechannel.
//!
//! The requester is declared as a policy at the mount site and injected into the handler as an
//! `Out` parameter, so it is live by construction; a small typed wrapper turns the raw
//! `RequestReply` capability into an encode-request / decode-reply call. The RPC timeout is the
//! failure boundary: no reply means the handler asks for redelivery instead of guessing.
//!
//! Start the inventory service first (`lapin_rpc_server`), then:
//!
//! ```text
//! cargo run --example lapin_rpc_client -- run
//! ```
//!
//! Place an order from another terminal:
//!
//! ```text
//! docker exec ruststream-rabbitmq rabbitmqadmin publish message \
//!   --routing-key orders --payload '{"sku":"widget","quantity":2}'
//! ```

use std::time::Duration;

use ruststream::codec::{Codec, JsonCodec};
use ruststream::runtime::{App, AppInfo, HandlerResult, Out, RustStream};
use ruststream::{IncomingMessage, OutgoingMessage, RequestReply, subscriber};
use ruststream_lapin::{LapinBroker, LapinRequest, LapinRequester};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
struct Order {
    sku: String,
    quantity: u32,
}

#[derive(Debug, Serialize)]
struct CheckStock {
    sku: String,
    quantity: u32,
}

#[derive(Debug, Deserialize)]
struct Stock {
    #[allow(dead_code)]
    sku: String,
    available: bool,
}

// --8<-- [start:client]
/// A typed RPC call over the raw requester: encode the request, await the correlated reply,
/// decode it.
async fn check_stock(
    requester: &LapinRequester,
    sku: &str,
    quantity: u32,
) -> Result<Stock, Box<dyn std::error::Error + Send + Sync>> {
    let request = CheckStock {
        sku: sku.to_owned(),
        quantity,
    };
    let payload = JsonCodec.encode(&request)?;
    let reply = requester
        .request(
            OutgoingMessage::new("inventory.check", payload.as_ref()),
            Duration::from_secs(2),
        )
        .await?;
    Ok(JsonCodec.decode(reply.payload())?)
}
// --8<-- [end:client]

// --8<-- [start:handler]
// The business handler calls the other service synchronously. A business answer settles the
// order either way; only an unreachable inventory service (the RPC timed out or failed) asks
// the broker to redeliver and try again later.
#[subscriber("orders")]
async fn place_order(order: &Order, Out(inventory): Out<LapinRequester>) -> HandlerResult {
    match check_stock(inventory, &order.sku, order.quantity).await {
        Ok(stock) if stock.available => {
            println!("order accepted: {} x{}", order.sku, order.quantity);
            HandlerResult::Ack
        }
        Ok(_) => {
            println!(
                "order rejected, out of stock: {} x{}",
                order.sku, order.quantity
            );
            HandlerResult::Ack
        }
        Err(err) => {
            eprintln!("inventory unavailable, retrying later: {err}");
            HandlerResult::retry()
        }
    }
}
// --8<-- [end:handler]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App {
    let broker = LapinBroker::new("amqp://localhost:5672").declare_topology(true);
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        // The requester is a policy here: the runtime pairs it with the connection at startup
        // and hands the handler the live client.
        b.include(place_order).publisher(LapinRequest::default());
    })
}
// --8<-- [end:app]
