//! Request/reply over `RabbitMQ` direct reply-to, expressed with the framework's publishing
//! form: the responder is an ordinary `#[subscriber(.., publish(..))]` handler, and a static
//! `PublishTransform` redirects each reply to the requester's private reply-to address.
//!
//! The requester side is [`LapinBroker::requester`]: it consumes the `amq.rabbitmq.reply-to`
//! pseudo-queue, stamps `reply-to` and a generated `correlation-id` on every request, and
//! matches replies back by correlation id.
//!
//! ```text
//! just brokers-up
//! cargo run --example lapin_request_reply
//! ```

use std::time::Duration;

use ruststream::runtime::{
    App, AppInfo, HandlerResult, Outgoing, PublishContext, PublishTransform, RustStream,
    TypedPublisher,
};
use ruststream::{IncomingMessage, OutgoingMessage, RequestReply, subscriber};
use ruststream_lapin::LapinBroker;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
struct Ping {
    id: u64,
}

#[derive(Debug, Serialize)]
struct Pong {
    id: u64,
}

// --8<-- [start:handler]
// The responder is a plain publishing handler: decode the request, return the reply. `Err`
// settles without replying (a request with no id is dropped, and the timeout on the requester
// side is the recovery mechanism).
#[subscriber("rpc.ping", publish("rpc.ping.unrouted"))]
async fn ping(req: &Ping) -> Result<Pong, HandlerResult> {
    if req.id == 0 {
        return Err(HandlerResult::drop());
    }
    Ok(Pong { id: req.id })
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

fn app(broker: LapinBroker) -> impl App {
    // --8<-- [start:mount]
    RustStream::new(AppInfo::new("rpc", "0.1.0")).with_broker(broker, |b| {
        let replies = TypedPublisher::new(b.broker().publisher()).transform(DirectReplyTo);
        b.include_publishing(ping, replies);
    })
    // --8<-- [end:mount]
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let broker = LapinBroker::new("amqp://localhost:5672").declare_topology(true);
    // Handed out before the runtime connects; it resolves the shared connection on first use.
    let requester = broker.requester();

    app(broker)
        .run_until(async move {
            // --8<-- [start:request]
            let reply = requester
                .request(
                    OutgoingMessage::new("rpc.ping", br#"{"id":7}"#),
                    Duration::from_secs(2),
                )
                .await
                .expect("reply within the timeout");
            println!("reply: {}", String::from_utf8_lossy(reply.payload()));
            // --8<-- [end:request]
        })
        .await?;
    Ok(())
}
