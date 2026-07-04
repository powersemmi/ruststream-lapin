//! The delivery type yielded by [`LapinSubscriber`](crate::LapinSubscriber).

use std::time::Duration;

use bytes::Bytes;
use lapin::Acker;
use lapin::message::Delivery;
use lapin::options::{BasicAckOptions, BasicNackOptions, BasicPublishOptions, BasicRejectOptions};
use lapin::types::ShortString;
use ruststream::{AckError, Headers, IncomingMessage, Partitioned};

use crate::convert;
use crate::delay::DelayContext;

/// Header carrying a message's partition key, read by [`Partitioned`] for keyed worker lanes.
///
/// Set it on an outgoing message's [`Headers`] to route deliveries that share a key to the same
/// worker lane under [`workers(n, by_key)`](https://docs.rs/ruststream). It rides in the AMQP
/// header table like any other header; nothing else in the broker interprets it.
pub const PARTITION_KEY_HEADER: &str = "amqp-partition-key";

/// One AMQP delivery, settled with the protocol's native acknowledgement frames.
///
/// Settlement mapping:
///
/// - [`ack`](IncomingMessage::ack) sends `basic.ack`.
/// - [`nack(true)`](IncomingMessage::nack) sends `basic.nack` with `requeue = true`; the broker
///   redelivers the message (typically to the same queue, `redelivered` set).
/// - [`nack(false)`](IncomingMessage::nack) sends `basic.reject` with `requeue = false`; the
///   broker drops the message, or dead-letters it when the queue has a dead-letter exchange.
/// - [`nack_after(delay)`](IncomingMessage::nack_after) is native only when the subscription set
///   [`RabbitQueue::delay`](crate::RabbitQueue::delay); otherwise the default reports the delay
///   unsupported and the runtime uses its broker-agnostic fallback.
///
/// Replies received through [`LapinRequester`](crate::LapinRequester) arrive on a no-ack
/// consumer; settling them is a no-op that always succeeds.
#[derive(Debug)]
pub struct LapinMessage {
    payload: Bytes,
    headers: Headers,
    exchange: String,
    routing_key: String,
    redelivered: bool,
    delivery_tag: u64,
    acker: Option<Acker>,
    delay: Option<DelayContext>,
}

impl LapinMessage {
    pub(crate) fn from_delivery(delivery: Delivery, delay: Option<DelayContext>) -> Self {
        let headers = convert::headers_from_properties(&delivery.properties);
        Self {
            payload: Bytes::from(delivery.data),
            headers,
            exchange: delivery.exchange.to_string(),
            routing_key: delivery.routing_key.to_string(),
            redelivered: delivery.redelivered,
            delivery_tag: delivery.delivery_tag,
            acker: Some(delivery.acker),
            delay,
        }
    }

    /// Builds a settled-by-construction message for no-ack deliveries (request replies).
    pub(crate) fn from_delivery_no_ack(delivery: Delivery) -> Self {
        let mut msg = Self::from_delivery(delivery, None);
        msg.acker = None;
        msg
    }

    /// The exchange this message was published to (empty for the default exchange).
    #[must_use]
    pub fn exchange(&self) -> &str {
        &self.exchange
    }

    /// The routing key the message was published with.
    #[must_use]
    pub fn routing_key(&self) -> &str {
        &self.routing_key
    }

    /// Whether the broker marked this delivery as redelivered.
    #[must_use]
    pub fn redelivered(&self) -> bool {
        self.redelivered
    }

    /// The channel-local delivery tag of this delivery.
    #[must_use]
    pub fn delivery_tag(&self) -> u64 {
        self.delivery_tag
    }

    async fn settle<F, Fut>(mut self, op: F, what: &'static str) -> Result<(), AckError>
    where
        F: FnOnce(Acker) -> Fut,
        Fut: Future<Output = lapin::Result<bool>>,
    {
        // No acker means a no-ack consumer delivered this message; nothing to settle.
        let Some(acker) = self.acker.take() else {
            return Ok(());
        };
        match op(acker).await {
            Ok(true) => Ok(()),
            // lapin reports `false` when the settle frame could not be sent because the channel
            // already closed or errored; surface that instead of pretending the broker saw it.
            Ok(false) => Err(AckError::Broker(
                format!("{what} was not sent: the delivery channel is closed or errored").into(),
            )),
            Err(err) => Err(AckError::Broker(Box::new(err))),
        }
    }
}

impl IncomingMessage for LapinMessage {
    fn payload(&self) -> &[u8] {
        &self.payload
    }

    fn headers(&self) -> &Headers {
        &self.headers
    }

    /// Acknowledges the delivery with `basic.ack`.
    ///
    /// # Errors
    ///
    /// Returns [`AckError::Broker`] when the frame cannot be sent, for example because the
    /// channel closed after the delivery arrived.
    ///
    /// # Cancel safety
    ///
    /// Not cancel safe: dropping the future after the frame was queued may still acknowledge the
    /// message on the broker.
    async fn ack(self) -> Result<(), AckError> {
        self.settle(
            |acker| async move { acker.ack(BasicAckOptions::default()).await },
            "basic.ack",
        )
        .await
    }

    /// Settles negatively: `basic.nack(requeue = true)` or `basic.reject(requeue = false)`.
    ///
    /// # Errors
    ///
    /// Returns [`AckError::Broker`] when the frame cannot be sent, for example because the
    /// channel closed after the delivery arrived.
    ///
    /// # Cancel safety
    ///
    /// Not cancel safe: dropping the future after the frame was queued may still settle the
    /// message on the broker.
    async fn nack(self, requeue: bool) -> Result<(), AckError> {
        if requeue {
            self.settle(
                |acker| async move {
                    acker
                        .nack(BasicNackOptions {
                            multiple: false,
                            requeue: true,
                        })
                        .await
                },
                "basic.nack",
            )
            .await
        } else {
            self.settle(
                |acker| async move { acker.reject(BasicRejectOptions { requeue: false }).await },
                "basic.reject",
            )
            .await
        }
    }

    /// The partition key from the [`PARTITION_KEY_HEADER`], if set. Overridden so keyed worker
    /// lanes see it without a `Partitioned` bound on every dispatch path.
    fn partition_key(&self) -> Option<&[u8]> {
        self.headers.get(PARTITION_KEY_HEADER)
    }

    /// Whether this delivery can honor a native delayed redelivery.
    ///
    /// `true` only when the subscription set [`RabbitQueue::delay`](crate::RabbitQueue::delay);
    /// otherwise the runtime uses its broker-agnostic deferred re-publish.
    fn supports_nack_after(&self) -> bool {
        self.delay.is_some()
    }

    /// Redelivers this message no sooner than `delay`, natively: re-publish it to the delay
    /// waiting queue with a per-message TTL, then acknowledge the original. The waiting queue
    /// dead-letters the copy back to the origin queue when the TTL fires.
    ///
    /// Duplicate-not-loss: the re-publish is sent on the same channel before the original is
    /// acked, so a connection failure between them leaves the original unacked (redelivered), not
    /// lost. The one loss window is a missing waiting queue - an unroutable publish to the default
    /// exchange is silently dropped - which is why the waiting queue is the user's declared
    /// infrastructure.
    ///
    /// # Errors
    ///
    /// Returns [`AckError::Unsupported`] when the subscription set no delay queue, and
    /// [`AckError::Broker`] when the re-publish or the ack fails.
    ///
    /// # Cancel safety
    ///
    /// Not cancel safe: dropping the future may leave the delayed copy published, the original
    /// acked, or both.
    async fn nack_after(mut self, delay: Duration) -> Result<(), AckError> {
        let Some(context) = self.delay.take() else {
            return Err(AckError::Unsupported);
        };
        let mut properties = convert::properties_for_publish(&self.headers, true)
            .map_err(|err| AckError::Broker(Box::new(err)))?;
        properties = properties.with_expiration(ShortString::from(DelayContext::expiration(delay)));

        context
            .channel()
            .basic_publish(
                ShortString::default(),
                ShortString::from(context.waiting_queue()),
                BasicPublishOptions::default(),
                &self.payload,
                properties,
            )
            .await
            .map_err(|err| AckError::Broker(Box::new(err)))?;

        self.settle(
            |acker| async move { acker.ack(BasicAckOptions::default()).await },
            "basic.ack",
        )
        .await
    }
}

impl Partitioned for LapinMessage {
    /// The partition key from the [`PARTITION_KEY_HEADER`], or `None` when unset.
    ///
    /// Deliveries that share a key are dispatched to the same worker lane under
    /// [`workers(n, by_key)`](https://docs.rs/ruststream); AMQP itself does not interpret the
    /// header, so the producer sets it.
    fn partition_key(&self) -> Option<&[u8]> {
        self.headers.get(PARTITION_KEY_HEADER)
    }
}
