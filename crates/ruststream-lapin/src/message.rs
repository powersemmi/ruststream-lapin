//! The delivery type yielded by [`LapinSubscriber`](crate::LapinSubscriber).

use std::time::Duration;

use bytes::Bytes;
use lapin::message::Delivery;
use lapin::options::{BasicAckOptions, BasicRejectOptions};
use lapin::types::{AMQPValue, FieldTable, ShortString};
use lapin::{Acker, BasicProperties};
use ruststream::{AckError, HeaderMap, IncomingMessage, Partitioned};

use crate::convert;
use crate::delay::DelayContext;

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
const DELIVERY_COUNT_HEADER: &str = "x-delivery-count";

/// The table `RabbitMQ` stamps on a message it dead-letters, one entry per queue it has left and
/// why.
const DEATH_HEADER: &str = "x-death";

/// The field of an `x-death` entry that carries how often that death has happened.
const COUNT: &str = "count";

/// The reason a waiting queue releases a delayed copy: its per-message TTL ran out.
const EXPIRED: &[u8] = b"expired";

/// The reason a queue dead-letters a delivery a consumer rejected.
const REJECTED: &[u8] = b"rejected";

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
    async fn ack(self) -> Result<(), AckError> {
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
    async fn nack(self, requeue: bool) -> Result<(), AckError> {
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

    /// How many times this message has been delivered, counting this delivery, from the two
    /// counters the server keeps: a quorum queue's `x-delivery-count` and the `x-death` table of a
    /// message the server has carried away and brought back. Where both are there the longer one
    /// answers.
    ///
    /// A quorum queue counts a delivery that failed - one whose consumer went away without
    /// settling it. A `basic.nack` with requeue asks for the message back instead, and `RabbitMQ`
    /// 4.3 does not count that where 4.2 did, so a handler-driven retry loop is bounded by the
    /// framework's own retry-count header rather than by this.
    ///
    /// `None` where neither counter is there: a classic queue that has not dead-lettered this
    /// message, and the first delivery of any message. That is what leaves the framework's header
    /// in charge of a registration's cap, and it is also what a delayed redelivery through
    /// [`RabbitQueue::delay`](crate::RabbitQueue::delay) reports, because the copy the waiting
    /// queue releases is a new message the server has counted once.
    fn redelivery_count(&self) -> Option<u64> {
        self.returns.map(|returns| returns.saturating_add(1))
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
    let table = properties.headers().as_ref()?;
    let counted = table
        .inner()
        .get(&ShortString::from(DELIVERY_COUNT_HEADER))
        .and_then(convert::counter);
    let dead_lettered = death_count(table);
    match (counted, dead_lettered) {
        (None, None) => None,
        // A queue that counts its failures and also dead-letters keeps both, and they count
        // different halves of the same journey; the longer one is how far the message has come.
        (counted, dead_lettered) => Some(counted.unwrap_or(0).max(dead_lettered.unwrap_or(0))),
    }
}

/// How often this message has left a queue and come back, from the `x-death` table.
///
/// Two reasons are summed because both are a round trip rather than an ending: `rejected` is a
/// queue carrying away a delivery a handler dropped, and `expired` is a waiting queue releasing a
/// message whose time was up. Every other reason (`maxlen`, `delivery_limit`) says the message
/// left for good, and a message that left counts no attempt.
///
/// The table is the server's own record, so it grows only where the server itself moved the
/// message: a delayed redelivery this crate publishes as a copy is a new message, and the server
/// writes the entry afresh for it. That is why a cap does not reach `retry_after` on a queue with
/// [`RabbitQueue::delay`](crate::RabbitQueue::delay), and why it does reach a delivery a queue
/// dead-lettered on its own.
fn death_count(table: &FieldTable) -> Option<u64> {
    let AMQPValue::FieldArray(entries) = table.inner().get(&ShortString::from(DEATH_HEADER))?
    else {
        return None;
    };
    let total: u64 = entries
        .as_slice()
        .iter()
        .filter_map(|entry| match entry {
            AMQPValue::FieldTable(entry) => Some(entry),
            _ => None,
        })
        .filter(|entry| {
            entry
                .inner()
                .get(&ShortString::from("reason"))
                .and_then(text_of)
                .is_some_and(|reason| reason == EXPIRED || reason == REJECTED)
        })
        .filter_map(|entry| {
            entry
                .inner()
                .get(&ShortString::from(COUNT))
                .and_then(convert::counter)
        })
        .sum();
    (total > 0).then_some(total)
}

/// The bytes of a header value that carries text, whichever string type the server chose.
fn text_of(value: &AMQPValue) -> Option<&[u8]> {
    match value {
        AMQPValue::LongString(value) => Some(value.as_bytes()),
        AMQPValue::ShortString(value) => Some(value.as_str().as_bytes()),
        _ => None,
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

#[cfg(test)]
mod tests {
    use lapin::BasicProperties;
    use lapin::types::{AMQPValue, FieldArray, FieldTable, ShortString};

    use super::{DEATH_HEADER, DELIVERY_COUNT_HEADER, returns_of};

    /// One `x-death` entry, as the server writes it.
    fn death(queue: &str, reason: &str, count: i64) -> AMQPValue {
        let mut entry = FieldTable::default();
        entry.insert(
            ShortString::from("queue"),
            AMQPValue::LongString(queue.into()),
        );
        entry.insert(
            ShortString::from("reason"),
            AMQPValue::LongString(reason.into()),
        );
        entry.insert(ShortString::from("count"), AMQPValue::LongLongInt(count));
        AMQPValue::FieldTable(entry)
    }

    fn deaths(entries: Vec<AMQPValue>) -> AMQPValue {
        AMQPValue::FieldArray(FieldArray::from(entries))
    }

    /// A delivery carrying `fields` in its header table.
    fn delivered(fields: &[(&str, AMQPValue)]) -> BasicProperties {
        let mut table = FieldTable::default();
        for (name, value) in fields {
            table.insert(ShortString::from(*name), value.clone());
        }
        BasicProperties::default().with_headers(table)
    }

    // Both reasons are a round trip, so both count towards the attempts a message has spent.
    #[test]
    fn a_dead_lettered_delivery_counts_every_round_it_has_been_through() {
        let properties = delivered(&[(
            DEATH_HEADER,
            deaths(vec![
                death("orders.retry", "expired", 2),
                death("orders", "rejected", 1),
            ]),
        )]);

        assert_eq!(returns_of(&properties), Some(3));
    }

    // A message dropped for a queue length or a delivery limit left for good; it did not come
    // round again, so it spent no attempt.
    #[test]
    fn a_death_that_is_not_a_round_trip_counts_for_nothing() {
        let properties = delivered(&[(
            DEATH_HEADER,
            deaths(vec![
                death("orders", "maxlen", 7),
                death("orders", "delivery_limit", 4),
            ]),
        )]);

        assert_eq!(returns_of(&properties), None);
    }

    // A quorum queue with a dead-letter route keeps both counters, and they count different
    // halves of the same journey: the longer one is how far the message has come.
    #[test]
    fn the_longer_of_the_two_counters_answers() {
        let properties = delivered(&[
            (DELIVERY_COUNT_HEADER, AMQPValue::LongLongInt(4)),
            (DEATH_HEADER, deaths(vec![death("orders", "rejected", 2)])),
        ]);

        assert_eq!(returns_of(&properties), Some(4));
    }

    // Nothing counted: the first delivery of a message on a queue that keeps no counter.
    #[test]
    fn a_delivery_with_neither_counter_reports_none() {
        assert_eq!(returns_of(&BasicProperties::default()), None);
        assert_eq!(returns_of(&delivered(&[])), None);
    }
}
