//! The delivery type yielded by [`LapinSubscriber`](crate::LapinSubscriber).

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use lapin::message::Delivery;
use lapin::options::{BasicAckOptions, BasicRejectOptions};
use lapin::types::ShortString;
use lapin::{Acker, BasicProperties, Channel};
use ruststream::{AckError, HeaderMap, IncomingMessage, Partitioned, Settle};

use crate::broker::AmqpConnection;
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
/// - [`ack`](Settle::ack) sends `basic.ack`.
/// - [`nack(true)`](Settle::nack) sends `basic.reject` with `requeue = true`; the broker
///   redelivers the message (typically to the same queue, `redelivered` set).
/// - [`nack(false)`](Settle::nack) sends `basic.reject` with `requeue = false`; the
///   broker drops the message, or dead-letters it when the queue has a dead-letter exchange.
///
/// Both settle with `basic.reject` rather than `basic.nack`. The two frames are the same
/// operation on a single delivery, and they differ in one thing that matters: `RabbitMQ` 4.3
/// counts a rejection as a delivery a message has spent and does not count a nack. A handler
/// asking for its message back is a failed attempt, so it is counted, and a quorum queue's
/// `x-delivery-limit` can then end a poison loop on the server.
/// - [`nack_after(delay)`](Settle::nack_after) is native only when the subscription set
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
    /// What a delivery of a live consumer settles with; `None` for a no-ack delivery, which has
    /// nothing to settle.
    live: Option<Live>,
    delay: Option<DelayContext>,
    /// How a delivery of the in-process transport settles, in place of the acker a live one
    /// carries. The field is there only with the `testing` feature.
    #[cfg(feature = "testing")]
    in_process: Option<Box<Settlement>>,
}

impl LapinMessage {
    pub(crate) fn from_delivery(
        delivery: Delivery,
        channel: Option<Arc<DeliveryChannel>>,
        delay: Option<DelayContext>,
    ) -> Self {
        let headers = convert::headers_from_properties(&delivery.properties);
        let live = channel.map(|channel| Live {
            acker: delivery.acker,
            channel,
        });
        Self {
            payload: Bytes::from(delivery.data),
            headers,
            exchange: delivery.exchange.to_string(),
            routing_key: delivery.routing_key.to_string(),
            redelivered: delivery.redelivered,
            delivery_tag: delivery.delivery_tag,
            returns: returns_of(&delivery.properties),
            live,
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
            live: None,
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
        Self::from_delivery(delivery, None, None)
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

    /// Refuses a settlement of a delivery held across the broker's shutdown.
    fn refuse_after_shutdown(&self, frame: Frame) -> Result<(), AckError> {
        self.live
            .as_ref()
            .map_or(Ok(()), |live| live.refuse_after_shutdown(frame))
    }

    async fn settle(mut self, frame: Frame) -> Result<(), AckError> {
        // No live settlement means a no-ack consumer delivered this message; nothing to settle.
        let Some(live) = self.live.take() else {
            return Ok(());
        };
        live.settle(self.delivery_tag, frame).await
    }
}

/// The channel a subscription's deliveries arrive on, shared by every delivery it yields.
///
/// A delivery settles with a frame on this channel, so the channel stays open until the last
/// delivery it carried is settled or dropped.
#[derive(Debug)]
pub(crate) struct DeliveryChannel {
    /// The connection the channel belongs to, read for whether the broker has shut down.
    connection: Arc<AmqpConnection>,
    channel: Channel,
}

impl DeliveryChannel {
    pub(crate) fn new(connection: Arc<AmqpConnection>, channel: Channel) -> Arc<Self> {
        Arc::new(Self {
            connection,
            channel,
        })
    }

    /// The connection the channel belongs to.
    #[cfg(feature = "testing")]
    pub(crate) fn connection(&self) -> &AmqpConnection {
        &self.connection
    }
}

/// What a delivery of a live consumer settles with.
#[derive(Debug)]
struct Live {
    /// Lapin's handle for the delivery, read for one thing: whether the channel the delivery
    /// arrived on has ended since.
    acker: Acker,
    channel: Arc<DeliveryChannel>,
}

impl Live {
    /// Refuses a settlement of a delivery held across the broker's shutdown.
    ///
    /// Why this stays a runtime check: a delivery aliases the connection and may outlive it, and
    /// its settlement after the shutdown has to fail rather than reach a connection lapin may
    /// still be closing or reconnecting.
    fn refuse_after_shutdown(&self, frame: Frame) -> Result<(), AckError> {
        if self.channel.connection.is_closed() {
            return Err(AckError::Broker(
                format!("{} was not sent: the broker has shut down", frame.method()).into(),
            ));
        }
        Ok(())
    }

    /// Sends the settlement's frame on the channel the delivery arrived on.
    ///
    /// The frame goes out on the channel itself rather than through the acker, which hands every
    /// settlement to a task of lapin's own at the cost of several allocations. What the acker
    /// guards stays guarded: lapin poisons the ackers of a channel's deliveries once the channel
    /// starts recovering, closes or fails, and a recovered channel numbers its deliveries from one
    /// again, so the tag of a delivery from before the recovery may name another delivery. The
    /// check and the frame happen in one poll, as in lapin's own settlement.
    async fn settle(self, delivery_tag: u64, frame: Frame) -> Result<(), AckError> {
        self.refuse_after_shutdown(frame)?;
        if self.acker.poisoned() {
            return Err(AckError::Broker(
                format!(
                    "{} was not sent: the delivery channel is closed or errored",
                    frame.method()
                )
                .into(),
            ));
        }
        let channel = &self.channel.channel;
        match frame {
            Frame::Ack => {
                channel
                    .basic_ack(delivery_tag, BasicAckOptions::default())
                    .await
            }
            Frame::Reject { requeue } => {
                channel
                    .basic_reject(delivery_tag, BasicRejectOptions { requeue })
                    .await
            }
        }
        .map_err(|err| AckError::Broker(Box::new(err)))
    }
}

/// The frame a delivery settles with.
#[derive(Debug, Clone, Copy)]
enum Frame {
    Ack,
    Reject { requeue: bool },
}

impl Frame {
    /// The AMQP method the frame carries, for the error that reports it unsent.
    const fn method(self) -> &'static str {
        match self {
            Self::Ack => "basic.ack",
            Self::Reject { .. } => "basic.reject",
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
    /// It is also what a delayed redelivery through
    /// [`RabbitQueue::delay`](crate::RabbitQueue::delay) reports, because the copy the waiting
    /// queue releases is a new message the server counts from zero.
    fn redelivery_count(&self) -> Option<u64> {
        self.returns.map(|returns| returns.saturating_add(1))
    }
}

impl Settle for LapinMessage {
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
        self.settle(Frame::Ack).await
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
        self.settle(Frame::Reject { requeue }).await
    }

    /// Whether this delivery can honor a native delayed redelivery.
    ///
    /// `true` only when the subscription set [`RabbitQueue::delay`](crate::RabbitQueue::delay);
    /// otherwise a `retry_after` requeues the delivery at once and the delay is dropped.
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
    /// lost. The server drops a copy sent to a waiting queue that does not exist, so a
    /// subscription whose broker does not declare topology checks for the waiting queue (or the
    /// delayed exchange) when it opens, and refuses to open without it.
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
        // Before the copy, so a refused settlement leaves no copy behind either.
        self.refuse_after_shutdown(Frame::Ack)?;
        context
            .republish(&self.payload, &self.headers, delay)
            .await
            .map_err(|err| AckError::Broker(Box::new(err)))?;

        self.settle(Frame::Ack).await
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
