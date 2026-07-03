//! Request/reply over `RabbitMQ` direct reply-to.
//!
//! The requester publishes with `reply-to` set to the `amq.rabbitmq.reply-to` pseudo-queue and
//! a generated `correlation-id`; the responder publishes its reply to the `reply-to` address it
//! received (a plain publish to the default exchange), echoing the `correlation-id`.
//!
//! Direct reply-to is at-most-once: a reply is lost if the requester's channel drops before it
//! arrives, and the per-request timeout is the recovery mechanism.
//!
//! ```text
//! just brokers-up
//! cargo run --example lapin_request_reply
//! ```

use std::time::Duration;

use futures::StreamExt;
use ruststream::{
    Broker, Headers, IncomingMessage, OutgoingMessage, Publisher, RequestReply, Subscriber,
};
use ruststream_lapin::{LapinBroker, RabbitQueue};

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let broker = LapinBroker::new("amqp://localhost:5672").declare_topology(true);
    Broker::connect(&broker).await?;

    // --8<-- [start:responder]
    // The responder is an ordinary subscriber: it reads the reply address and the correlation
    // id off the request headers and publishes the reply through a plain publisher.
    let mut responder = broker
        .subscribe(RabbitQueue::new("rpc.ping").durable(false).exclusive(true))
        .await?;
    let reply_publisher = broker.publisher();
    tokio::spawn(async move {
        let mut requests = std::pin::pin!(responder.stream());
        while let Some(Ok(request)) = requests.next().await {
            let Some(reply_to) = request.headers().reply_to().map(str::to_owned) else {
                continue;
            };
            let mut headers = Headers::new();
            if let Some(correlation_id) = request.headers().correlation_id() {
                headers.insert("correlation-id", correlation_id.as_bytes().to_vec());
            }
            let reply = OutgoingMessage::new(&reply_to, b"pong").with_headers(headers);
            let _ = reply_publisher.publish(reply).await;
            let _ = request.ack().await;
        }
    });
    // --8<-- [end:responder]

    // --8<-- [start:request]
    let requester = broker.requester();
    let reply = requester
        .request(
            OutgoingMessage::new("rpc.ping", b"ping"),
            Duration::from_secs(2),
        )
        .await?;
    println!("reply: {}", String::from_utf8_lossy(reply.payload()));
    // --8<-- [end:request]

    broker.shutdown().await?;
    Ok(())
}
