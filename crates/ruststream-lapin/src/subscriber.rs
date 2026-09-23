//! The subscriber: a stream of AMQP deliveries from one queue consumer, batched on the client for
//! the handlers that take a batch.

use std::num::NonZeroUsize;
use std::time::Duration;

#[cfg(feature = "testing")]
use futures::future::Either;
use futures::{Stream, StreamExt};
use lapin::{Channel, Consumer};
use ruststream::{BatchSubscriber, BufferedSubscriber, Subscriber};

use crate::delay::DelayContext;
use crate::error::AmqpError;
#[cfg(feature = "testing")]
use crate::in_process::BusDeliveries;
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
/// It is a [`BatchSubscriber`] as well as a [`Subscriber`]: AMQP has no wire batch, so a batch
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
        batch_wait: Duration,
        delay: Option<DelayContext>,
    ) -> Self {
        let deliveries = Deliveries::Amqp(AmqpDeliveries {
            _channel: channel,
            consumer,
            delay,
        });
        Self {
            deliveries: BufferedSubscriber::new(deliveries).max_wait(batch_wait),
            queue,
        }
    }

    /// A subscriber on the in-process transport, batching on the client with the descriptor's
    /// own deadline as the live subscriber does.
    #[cfg(feature = "testing")]
    pub(crate) fn in_process(
        deliveries: BusDeliveries,
        queue: String,
        batch_wait: Duration,
    ) -> Self {
        Self {
            deliveries: BufferedSubscriber::new(Deliveries::InProcess(deliveries))
                .max_wait(batch_wait),
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

/// Batches are assembled on the client: AMQP delivers one `basic.deliver` at a time, so there is
/// no wire batch to ask the broker for, and a batch closes on the registration's size or on the
/// descriptor's [`batch_wait`](crate::RabbitQueue::batch_wait), whichever comes first.
///
/// A batch can only hold what the broker has already pushed, so a subscription whose
/// [`prefetch`](crate::RabbitQueue::prefetch) window is narrower than the registration's batch size
/// yields batches capped by the window rather than by the size.
impl BatchSubscriber for LapinSubscriber {
    type Batch = Vec<LapinMessage>;

    /// # Cancel safety
    ///
    /// As cancel safe as [`stream`](Subscriber::stream) between polls; dropping the returned
    /// stream abandons the batch being assembled, and the broker redelivers those deliveries when
    /// the channel closes.
    fn batches(
        &mut self,
        size: NonZeroUsize,
    ) -> impl Stream<Item = Result<Self::Batch, <Self as Subscriber>::Error>> + Send + '_ {
        self.deliveries.batches(size)
    }
}

/// One delivery at a time, from whichever transport the broker was connected to: everything above
/// it batches on the client.
///
/// Without the `testing` feature the wire consumer is the only variant, so the type is the
/// consumer and the `match` in [`stream`](Subscriber::stream) resolves at compile time.
// The live consumer is the larger variant on purpose: it is the one a production build has, and
// boxing it would put an allocation on the service's own path to shrink a test build.
#[cfg_attr(feature = "testing", allow(clippy::large_enum_variant))]
enum Deliveries {
    Amqp(AmqpDeliveries),
    #[cfg(feature = "testing")]
    InProcess(BusDeliveries),
}

#[cfg(not(feature = "testing"))]
const _: () = assert!(size_of::<Deliveries>() == size_of::<AmqpDeliveries>());

impl Subscriber for Deliveries {
    type Message = LapinMessage;
    type Error = AmqpError;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        match self {
            #[cfg(not(feature = "testing"))]
            Self::Amqp(consumer) => consumer.stream(),
            #[cfg(feature = "testing")]
            Self::Amqp(consumer) => Either::Left(consumer.stream()),
            #[cfg(feature = "testing")]
            Self::InProcess(deliveries) => Either::Right(deliveries.stream()),
        }
    }
}

/// The wire consumer, one `basic.deliver` at a time.
struct AmqpDeliveries {
    // Kept alive for the lifetime of the subscription: dropping the channel cancels the
    // consumer server-side.
    _channel: Channel,
    consumer: Consumer,
    // Present only when the descriptor opted into a native delay queue; threaded into every
    // delivery so `nack_after` can re-publish to the waiting queue.
    delay: Option<DelayContext>,
}

impl AmqpDeliveries {
    fn stream(&mut self) -> impl Stream<Item = Result<LapinMessage, AmqpError>> + Send + '_ {
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
