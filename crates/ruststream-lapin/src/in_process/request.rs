//! Request/reply on the in-process transport: the direct reply-to convention over a private reply
//! address per request.

use std::sync::Arc;
use std::time::Duration;

use ruststream::{HeaderMap, OutgoingMessage};
use tokio::time::{Instant, timeout_at};
use tracing::debug;

use super::bus::{Bus, ConsumerId, DeliveryReceiver};
use super::deliveries::Settlement;
use crate::error::AmqpError;
use crate::message::LapinMessage;
use crate::publish_policy::PublishOptions;
use crate::requester::REPLY_TO;

/// The private reply address of one in-flight request.
///
/// `RabbitMQ` rewrites the direct reply-to pseudo-queue per request, so a responder sees an
/// address of its own for every question it is asked; the transport does the same, with a consumer
/// that lives exactly as long as the request waiting on it.
struct Inbox {
    bus: Arc<Bus>,
    id: ConsumerId,
    address: String,
    correlation_id: String,
    replies: DeliveryReceiver,
}

impl Inbox {
    fn open(bus: &Arc<Bus>) -> Self {
        let seq = bus.next_inbox();
        let address = format!("{REPLY_TO}.{seq}");
        let (id, replies) = bus.consume(address.clone());
        Self {
            bus: Arc::clone(bus),
            id,
            address,
            correlation_id: format!("rs-{seq}"),
            replies,
        }
    }

    /// Balances the harness's count for a delivery this request does not hand back.
    fn discard(&self) {
        if let Some(coordinator) = self.bus.coordinator() {
            coordinator.consumed();
        }
    }
}

impl Drop for Inbox {
    fn drop(&mut self) {
        self.bus.cancel(self.id);
        // A reply that arrived while the request was giving up was counted in on its way here;
        // draining it keeps the harness's in-flight count balanced.
        while self.replies.try_recv().is_ok() {
            self.discard();
        }
    }
}

/// Sends `msg` with a private reply address and a correlation id, and waits for the reply that
/// carries that id back.
///
/// # Errors
///
/// Returns [`AmqpError::RequestTimeout`] when no correlated reply arrives within `timeout`,
/// [`AmqpError::Closed`] once the connection has shut down (including under a waiting request),
/// and what [`Bus::publish`] refuses.
pub(crate) async fn request(
    bus: &Arc<Bus>,
    options: &PublishOptions,
    msg: OutgoingMessage<'_>,
    timeout: Duration,
) -> Result<LapinMessage, AmqpError> {
    bus.ensure_live(msg.name())?;
    let mut inbox = Inbox::open(bus);
    let mut headers: HeaderMap = msg.headers().clone();
    headers.insert("reply-to", inbox.address.clone());
    headers.insert("correlation-id", inbox.correlation_id.clone());
    // A request has no call site to adjust its settings: the policy's are the whole answer, as on
    // the live requester.
    bus.publish(
        &options.exchange,
        msg.name(),
        msg.payload(),
        &headers,
        &options.resolve(None),
    )?;

    // One deadline for the whole wait: an uncorrelated reply must not buy the request another
    // full timeout.
    let deadline = Instant::now() + timeout;
    let mut tag = 1;
    loop {
        let Ok(received) = timeout_at(deadline, inbox.replies.recv()).await else {
            return Err(AmqpError::RequestTimeout(timeout));
        };
        let Some(delivery) = received else {
            // The transport dropped the consumer: the connection shut down under the request.
            return Err(AmqpError::closed(&inbox.address));
        };
        if delivery.headers.correlation_id() == Some(inbox.correlation_id.as_str()) {
            let settlement = Settlement::no_ack(&inbox.bus, inbox.address.clone());
            return Ok(LapinMessage::in_process(delivery, tag, settlement));
        }
        tag += 1;
        // The live requester multiplexes one reply consumer across its requests, and a delivery
        // that does not correlate is not this request's answer there either.
        debug!(
            target: "ruststream_lapin",
            address = inbox.address,
            "dropping a reply that does not correlate with the pending request"
        );
        inbox.discard();
    }
}
