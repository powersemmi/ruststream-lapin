//! The delivery type yielded by [`LapinSubscriber`](crate::LapinSubscriber).

use std::time::Duration;

use bytes::Bytes;
use lapin::message::Delivery;
use lapin::options::{BasicAckOptions, BasicRejectOptions};
use lapin::types::ShortString;
use lapin::{Acker, BasicProperties};
use ruststream::{AckError, HeaderMap, IncomingMessage, Partitioned};

use crate::convert;
use crate::delay::DelayContext;
#[cfg(feature = "testing")]
use crate::in_process::{BusDelivery, Settlement};

/// Header carrying a message's partition key, read by [`Partitioned`] for keyed worker lanes.
///
/// Set it on an outgoing message's [`HeaderMap`] to route deliveries that share a key to the same
/// worker lane under [`workers(n, by_key)`](https://docs.rs/ruststream). It rides in the AMQP
/// header table like any other header; nothing else in the broker interprets it.
pub const PARTITION_KEY_HEADER: &str = "amqp-partition-key";

/// The header a quorum queue stamps with how often it has returned this message to the queue.
///
/// Classic queues keep no such counter, and a quorum queue omits the header until the message has
/// been returned at least once, so its absence says "no count", not "one delivery".
pub(crate) const DELIVERY_COUNT_HEADER: &str = "x-delivery-count";

/// One AMQP delivery, settled with the protocol's native acknowledgement frames.
///
/// Settlement mapping:
///
/// - [`ack`](IncomingMessage::ack) sends `basic.ack`.
/// - [`nack(true)`](IncomingMessage::nack) sends `basic.reject` with `requeue = true`; the broker
///   redelivers the message (typically to the same queue, `redelivered` set).
/// - [`nack(false)`](IncomingMessage::nack) sends `basic.reject` with `requeue = false`; the
///   broker drops the message, or dead-letters it when the queue has a dead-letter exchange.
///
/// Both settle with `basic.reject` rather than `basic.nack`. The two frames are the same
/// operation on a single delivery, and they differ in one thing that matters: `RabbitMQ` 4.3
/// counts a rejection as a delivery a message has spent and does not count a nack. A handler
/// asking for its message back is a failed attempt, so it is counted, and a quorum queue's
/// `x-delivery-limit` can then end a poison loop on the server.
/// - [`nack_after(delay)`](IncomingMessage::nack_after) is native only when the subscription set
///   [`RabbitQueue::delay`](crate::RabbitQueue::delay); otherwise the default reports the delay
///   unsupported and the runtime uses its broker-agnostic fallback.
///
/// Replies received through [`LapinRequester`](crate::LapinRequester) arrive on a no-ack
/// consumer; settling them is a no-op that always succeeds.
#[derive(Debug)]
pub struct LapinMessage {
    payload: Bytes,
    headers: HeaderMap,
    exchange: String,
    routing_key: String,
    redelivered: bool,
    delivery_tag: u64,
    returns: Option<u64>,
    acker: Option<Acker>,
    delay: Option<DelayContext>,
    /// How a delivery of the in-process transport settles, in place of the acker a live one
    /// carries. The field is there only with the `testing` feature.
    #[cfg(feature = "testing")]
    in_process: Option<Box<Settlement>>,
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
            returns: returns_of(&delivery.properties),
            acker: Some(delivery.acker),
            delay,
            #[cfg(feature = "testing")]
            in_process: None,
        }
    }

    /// A delivery of the in-process transport, reporting what a live one reports.
    #[cfg(feature = "testing")]
    pub(crate) fn in_process(delivery: BusDelivery, delivery_tag: u64, settle: Settlement) -> Self {
        Self {
            payload: delivery.payload,
            headers: delivery.headers,
            exchange: delivery.exchange,
            routing_key: delivery.routing_key,
            redelivered: delivery.redelivered,
            delivery_tag,
            returns: delivery.returns,
            acker: None,
            delay: None,
            in_process: Some(Box::new(settle)),
        }
    }

    /// The delivery as the in-process transport routes it again, for a requeue or a redelivery.
    #[cfg(feature = "testing")]
    fn into_bus_delivery(self) -> BusDelivery {
        BusDelivery {
            payload: self.payload,
            headers: self.headers,
            exchange: self.exchange,
            routing_key: self.routing_key,
            redelivered: self.redelivered,
            returns: self.returns,
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

    fn headers(&self) -> &HeaderMap {
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
    async fn ack(
        // The binding is mutated only to take the in-process settlement out.
        #[cfg_attr(not(feature = "testing"), allow(unused_mut))] mut self,
    ) -> Result<(), AckError> {
        #[cfg(feature = "testing")]
        if let Some(settle) = self.in_process.take() {
            return settle.ack();
        }
        self.settle(
            |acker| async move { acker.ack(BasicAckOptions::default()).await },
            "basic.ack",
        )
        .await
    }

    /// Settles negatively with `basic.reject`: back to the queue under `requeue = true`, away
    /// from it under `requeue = false`.
    ///
    /// The frame is a rejection in both cases, never `basic.nack`. On one delivery the two are
    /// the same operation - `basic.nack` only adds the `multiple` flag this crate never sets -
    /// but a quorum queue counts a rejected delivery and does not count a nacked one, so a
    /// handler's `retry()` spends an attempt the server can see and its `x-delivery-limit` ends
    /// the loop.
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
    async fn nack(
        // The binding is mutated only to take the in-process settlement out.
        #[cfg_attr(not(feature = "testing"), allow(unused_mut))] mut self,
        requeue: bool,
    ) -> Result<(), AckError> {
        #[cfg(feature = "testing")]
        if let Some(settle) = self.in_process.take() {
            return settle.reject(self.into_bus_delivery(), requeue);
        }
        self.settle(
            |acker| async move { acker.reject(BasicRejectOptions { requeue }).await },
            "basic.reject",
        )
        .await
    }

    /// The partition key from the [`PARTITION_KEY_HEADER`], if set. Overridden so keyed worker
    /// lanes see it without a `Partitioned` bound on every dispatch path.
    fn partition_key(&self) -> Option<&[u8]> {
        self.headers.get(PARTITION_KEY_HEADER)
    }

    /// How many times this message has been delivered, counting this delivery, from the one
    /// counter the server keeps: a quorum queue's `x-delivery-count`.
    ///
    /// A quorum queue counts a delivery that failed - one whose consumer went away without
    /// settling it, and one this crate rejected for a handler that asked for the message back.
    ///
    /// `None` where nothing counted: every delivery off a classic queue, which keeps no counter,
    /// and the first delivery of any message. A classic queue's `redelivered` flag is not a count,
    /// and the `x-death` table a dead-lettered message carries counts the queues it has left
    /// rather than the deliveries it has spent, so neither stands in for the server's own count.
    /// That is what leaves the framework's header in charge of a classic queue's cap, and it is
    /// also what a delayed redelivery through [`RabbitQueue::delay`](crate::RabbitQueue::delay)
    /// reports, because the copy the waiting queue releases is a new message the server counts
    /// from zero.
    fn redelivery_count(&self) -> Option<u64> {
        self.returns.map(|returns| returns.saturating_add(1))
    }

    /// Whether this delivery can honor a native delayed redelivery.
    ///
    /// `true` only when the subscription set [`RabbitQueue::delay`](crate::RabbitQueue::delay);
    /// otherwise the runtime uses its broker-agnostic deferred re-publish.
    fn supports_nack_after(&self) -> bool {
        #[cfg(feature = "testing")]
        if let Some(settle) = &self.in_process {
            return settle.delays();
        }
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
        #[cfg(feature = "testing")]
        if let Some(settle) = self.in_process.take() {
            return settle.nack_after(self.into_bus_delivery(), delay);
        }
        let Some(context) = self.delay.take() else {
            return Err(AckError::Unsupported);
        };
        context
            .republish(&self.payload, &self.headers, delay)
            .await
            .map_err(|err| AckError::Broker(Box::new(err)))?;

        self.settle(
            |acker| async move { acker.ack(BasicAckOptions::default()).await },
            "basic.ack",
        )
        .await
    }
}

/// The `x-delivery-count` a quorum queue stamps, or `None` where nothing counted.
fn returns_of(properties: &BasicProperties) -> Option<u64> {
    properties
        .headers()
        .as_ref()?
        .inner()
        .get(&ShortString::from(DELIVERY_COUNT_HEADER))
        .and_then(convert::counter)
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

#[cfg(test)]
mod tests {
    use lapin::BasicProperties;
    use lapin::types::{AMQPValue, FieldArray, FieldTable, ShortString};

    use super::{DELIVERY_COUNT_HEADER, returns_of};

    /// A delivery carrying `fields` in its header table.
    fn delivered(fields: &[(&str, AMQPValue)]) -> BasicProperties {
        let mut table = FieldTable::default();
        for (name, value) in fields {
            table.insert(ShortString::from(*name), value.clone());
        }
        BasicProperties::default().with_headers(table)
    }

    // The queue's own counter is the count, whichever integer width the server wrote it in.
    #[test]
    fn the_queues_counter_is_the_count() {
        let properties = delivered(&[(DELIVERY_COUNT_HEADER, AMQPValue::LongLongInt(4))]);

        assert_eq!(returns_of(&properties), Some(4));
    }

    // The `x-death` table is a record of the queues a message has left, not of the deliveries it
    // has spent, so a message the server carried away and brought back still reports the queue's
    // count and nothing else.
    #[test]
    fn a_dead_lettered_delivery_reports_no_count_of_its_own() {
        let mut entry = FieldTable::default();
        entry.insert(
            ShortString::from("reason"),
            AMQPValue::LongString("rejected".into()),
        );
        entry.insert(ShortString::from("count"), AMQPValue::LongLongInt(2));
        let properties = delivered(&[(
            "x-death",
            AMQPValue::FieldArray(FieldArray::from(vec![AMQPValue::FieldTable(entry)])),
        )]);

        assert_eq!(returns_of(&properties), None);
    }

    // Nothing counted: the first delivery of a message, and every delivery on a classic queue.
    #[test]
    fn a_delivery_with_no_counter_reports_none() {
        assert_eq!(returns_of(&BasicProperties::default()), None);
        assert_eq!(returns_of(&delivered(&[])), None);
    }
}
