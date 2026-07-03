//! The subscriber: a stream of AMQP deliveries from one queue consumer.

use futures::{Stream, StreamExt};
use lapin::{Channel, Consumer};
use ruststream::Subscriber;

use crate::error::AmqpError;
use crate::message::LapinMessage;

/// A consumer on one queue, yielding [`LapinMessage`] deliveries.
///
/// Created by subscribing a [`RabbitQueue`](crate::RabbitQueue) descriptor (or a bare queue name)
/// through [`LapinBroker`](crate::LapinBroker). The subscriber owns a dedicated channel;
/// dropping it closes that channel and the broker redelivers whatever was unacknowledged.
///
/// Back-pressure: the broker stops pushing once
/// [`prefetch`](crate::LapinBroker::prefetch) unacknowledged deliveries are in flight, so
/// consuming slower slows the producer side down instead of buffering without bound.
pub struct LapinSubscriber {
    // Kept alive for the lifetime of the subscription: dropping the channel cancels the
    // consumer server-side.
    _channel: Channel,
    consumer: Consumer,
    queue: String,
}

impl LapinSubscriber {
    pub(crate) fn new(channel: Channel, consumer: Consumer, queue: String) -> Self {
        Self {
            _channel: channel,
            consumer,
            queue,
        }
    }

    /// The queue this subscriber consumes from.
    #[must_use]
    pub fn queue(&self) -> &str {
        &self.queue
    }
}

impl std::fmt::Debug for LapinSubscriber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LapinSubscriber")
            .field("queue", &self.queue)
            .finish_non_exhaustive()
    }
}

impl Subscriber for LapinSubscriber {
    type Message = LapinMessage;
    type Error = AmqpError;

    /// Streams deliveries as they arrive; the stream ends when the consumer is cancelled or the
    /// connection closes.
    ///
    /// # Cancel safety
    ///
    /// Polling is cancel safe (no delivery is lost by dropping the stream between polls), and
    /// the stream can be re-created by calling `stream` again: deliveries buffer in the
    /// consumer, not in the returned stream.
    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        futures::stream::unfold(&mut self.consumer, |consumer| async move {
            let item = consumer.next().await?;
            let mapped = match item {
                Ok(delivery) => Ok(LapinMessage::from_delivery(delivery)),
                Err(err) => Err(AmqpError::consume(err)),
            };
            Some((mapped, consumer))
        })
    }
}
