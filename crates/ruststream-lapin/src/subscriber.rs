//! The subscriber: a stream of AMQP deliveries from one queue consumer, paged on the client for
//! the handlers that take a page.

use std::num::NonZeroUsize;
use std::time::Duration;

use futures::{Stream, StreamExt};
use lapin::{Channel, Consumer};
use ruststream::{BatchSubscriber, BufferedSubscriber, Subscriber};

use crate::delay::DelayContext;
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
///
/// It is a [`BatchSubscriber`] as well as a [`Subscriber`]: AMQP has no wire batch, so a page
/// handler's size is honoured by collecting the deliveries here (see the
/// [impl](#impl-BatchSubscriber-for-LapinSubscriber)).
pub struct LapinSubscriber {
    deliveries: BufferedSubscriber<Deliveries>,
    queue: String,
}

impl LapinSubscriber {
    pub(crate) fn new(
        channel: Channel,
        consumer: Consumer,
        queue: String,
        page_wait: Duration,
        delay: Option<DelayContext>,
    ) -> Self {
        let deliveries = Deliveries {
            _channel: channel,
            consumer,
            delay,
        };
        Self {
            deliveries: BufferedSubscriber::new(deliveries).max_wait(page_wait),
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
        self.deliveries.stream()
    }
}

/// Pages are assembled on the client: AMQP delivers one `basic.deliver` at a time, so there is no
/// wire batch to ask the broker for, and a page closes on the registration's size or on the
/// descriptor's [`page_wait`](crate::RabbitQueue::page_wait), whichever comes first.
///
/// A page can only hold what the broker has already pushed, so a subscription whose
/// [`prefetch`](crate::RabbitQueue::prefetch) window is narrower than the registration's page size
/// yields pages capped by the window rather than by the size.
impl BatchSubscriber for LapinSubscriber {
    type Batch = Vec<LapinMessage>;

    /// # Cancel safety
    ///
    /// As cancel safe as [`stream`](Subscriber::stream) between polls; dropping the returned
    /// stream abandons the page being assembled, and the broker redelivers those deliveries when
    /// the channel closes.
    fn batches(
        &mut self,
        size: NonZeroUsize,
    ) -> impl Stream<Item = Result<Self::Batch, <Self as Subscriber>::Error>> + Send + '_ {
        self.deliveries.batches(size)
    }
}

/// The wire consumer, one `basic.deliver` at a time: everything above it pages on the client.
struct Deliveries {
    // Kept alive for the lifetime of the subscription: dropping the channel cancels the
    // consumer server-side.
    _channel: Channel,
    consumer: Consumer,
    // Present only when the descriptor opted into a native delay queue; threaded into every
    // delivery so `nack_after` can re-publish to the waiting queue.
    delay: Option<DelayContext>,
}

impl Subscriber for Deliveries {
    type Message = LapinMessage;
    type Error = AmqpError;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        let delay = self.delay.clone();
        futures::stream::unfold(
            (&mut self.consumer, delay),
            |(consumer, delay)| async move {
                let item = consumer.next().await?;
                let mapped = match item {
                    Ok(delivery) => Ok(LapinMessage::from_delivery(delivery, delay.clone())),
                    Err(err) => Err(AmqpError::consume(err)),
                };
                Some((mapped, (consumer, delay)))
            },
        )
    }
}
