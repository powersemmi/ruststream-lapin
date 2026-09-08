//! The in-process subscriber and its delivery type.

use std::future::{Future, ready};
use std::num::NonZeroUsize;
use std::sync::Arc;

use futures::Stream;
use ruststream::testing::Coordinator;
use ruststream::{
    AckError, BatchSubscriber, BufferedSubscriber, HeaderMap, IncomingMessage, Partitioned,
    Subscriber,
};

use super::broker::TestBrokerState;
use super::router::{DeliveryReceiver, SubscriptionId, TestDelivery};
use crate::error::AmqpError;

/// In-process subscriber on one queue name.
///
/// Yielded messages settle like the real transport: ack finalizes, `nack(true)` re-enqueues to
/// this same subscription, `nack(false)` drops. Batches are assembled on the client, exactly as
/// the real subscriber assembles them, so a batch handler mounts in process unchanged.
pub struct LapinTestSubscriber {
    deliveries: BufferedSubscriber<TestDeliveries>,
    queue: String,
}

impl LapinTestSubscriber {
    pub(crate) fn open(state: &Arc<TestBrokerState>, queue: String) -> Self {
        let (id, sender, receiver) = state.router.subscribe(queue.clone());
        // The router keeps its own clone; holding a second one here would keep the delivery
        // channel open past a shutdown that cleared the subscription table.
        drop(sender);
        let coordinator = state.coordinator();
        let deliveries = TestDeliveries {
            state: Arc::clone(state),
            id,
            queue: queue.clone(),
            receiver,
            coordinator,
            next_tag: 1,
        };
        Self {
            // The framework's own short deadline, not the descriptor's `batch_wait`: that one is
            // tuned against a network the in-process transport does not have, and a test should
            // not pay it per partial batch.
            deliveries: BufferedSubscriber::new(deliveries),
            queue,
        }
    }

    /// The queue this subscriber consumes from.
    #[must_use]
    pub fn queue(&self) -> &str {
        &self.queue
    }
}

impl std::fmt::Debug for LapinTestSubscriber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LapinTestSubscriber")
            .field("queue", &self.queue)
            .finish_non_exhaustive()
    }
}

impl Subscriber for LapinTestSubscriber {
    type Message = LapinTestMessage;
    type Error = AmqpError;

    /// Streams injected deliveries; never yields an error.
    ///
    /// # Cancel safety
    ///
    /// Cancel safe and re-enterable: the receiver is polled in place, so dropping the returned
    /// stream loses nothing and `stream` can be called again.
    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        self.deliveries.stream()
    }
}

impl BatchSubscriber for LapinTestSubscriber {
    type Batch = Vec<LapinTestMessage>;

    /// # Cancel safety
    ///
    /// As cancel safe as [`stream`](Subscriber::stream) between polls; dropping the returned
    /// stream abandons the batch being assembled.
    fn batches(
        &mut self,
        size: NonZeroUsize,
    ) -> impl Stream<Item = Result<Self::Batch, <Self as Subscriber>::Error>> + Send + '_ {
        self.deliveries.batches(size)
    }
}

/// The transport-side half: one injected delivery at a time, which is what the client batches
/// over.
struct TestDeliveries {
    state: Arc<TestBrokerState>,
    id: SubscriptionId,
    queue: String,
    receiver: DeliveryReceiver,
    coordinator: Option<Coordinator>,
    /// The next channel-local delivery tag. AMQP numbers deliveries per channel from 1, and a
    /// requeued message is handed out under a new tag.
    next_tag: u64,
}

impl Drop for TestDeliveries {
    fn drop(&mut self) {
        self.state.router.unsubscribe(self.id);
    }
}

impl Subscriber for TestDeliveries {
    type Message = LapinTestMessage;
    type Error = AmqpError;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        let Self {
            state,
            receiver,
            coordinator,
            queue,
            next_tag,
            ..
        } = self;
        futures::stream::poll_fn(move |cx| {
            receiver.poll_recv(cx).map(|delivery| {
                delivery.map(|delivery| {
                    let delivery_tag = *next_tag;
                    *next_tag += 1;
                    Ok(LapinTestMessage {
                        redelivered: delivery.redelivered,
                        delivery: Some(delivery),
                        queue: queue.clone(),
                        delivery_tag,
                        state: Arc::clone(state),
                        coordinator: coordinator.clone(),
                    })
                })
            })
        })
    }
}

/// One in-process delivery.
///
/// It reports the same delivery metadata as [`LapinMessage`](crate::LapinMessage), read against
/// the transport's own model - exact queue-name routing off the default exchange - so a handler
/// binding AMQP fields ([`AmqpContext`](crate::context::AmqpContext) and its `Ctx` keys) mounts
/// in process exactly as it does on a real server.
pub struct LapinTestMessage {
    delivery: Option<TestDelivery>,
    queue: String,
    delivery_tag: u64,
    redelivered: bool,
    state: Arc<TestBrokerState>,
    coordinator: Option<Coordinator>,
}

impl LapinTestMessage {
    /// Wraps a delivery that reached a caller directly rather than through a subscription: the
    /// reply of a [`LapinTestRequester`](super::LapinTestRequester) request.
    ///
    /// Direct reply-to is a no-ack consumer, so the settle calls have nothing to do; the queue
    /// this names is the request's private reply address, which the requester closes as it
    /// returns, so a `nack(requeue = true)` finds no consumer and the delivery ends there.
    pub(crate) fn from_reply(
        state: &Arc<TestBrokerState>,
        queue: String,
        delivery_tag: u64,
        delivery: TestDelivery,
    ) -> Self {
        Self {
            redelivered: delivery.redelivered,
            delivery: Some(delivery),
            queue,
            delivery_tag,
            state: Arc::clone(state),
            coordinator: state.coordinator(),
        }
    }

    fn take(&mut self) -> TestDelivery {
        // The settle methods consume `self`, so a second settle cannot compile; reaching this
        // twice is an internal invariant violation.
        self.delivery
            .take()
            .expect("LapinTestMessage settled twice")
    }

    /// The exchange this message was published to: always the default exchange, since the
    /// transport routes by exact queue name and models nothing else.
    #[must_use]
    pub const fn exchange(&self) -> &'static str {
        ""
    }

    /// The routing key the message was published with, which on the default exchange is the name
    /// of the queue it landed on.
    #[must_use]
    pub fn routing_key(&self) -> &str {
        &self.queue
    }

    /// Whether this delivery is a `nack(requeue = true)` coming back.
    #[must_use]
    pub const fn redelivered(&self) -> bool {
        self.redelivered
    }

    /// The channel-local delivery tag: subscriptions number their deliveries from 1, and a
    /// requeued message is handed out again under a new tag.
    #[must_use]
    pub const fn delivery_tag(&self) -> u64 {
        self.delivery_tag
    }
}

impl Drop for LapinTestMessage {
    fn drop(&mut self) {
        // Balance the router's `enqueued` exactly once per delivery, whatever the dispatch
        // path did (ack, nack, panic, or plain drop).
        if let Some(coordinator) = self.coordinator.take() {
            coordinator.consumed();
        }
    }
}

impl std::fmt::Debug for LapinTestMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LapinTestMessage")
            .field("delivery", &self.delivery)
            .finish_non_exhaustive()
    }
}

impl IncomingMessage for LapinTestMessage {
    fn payload(&self) -> &[u8] {
        &self
            .delivery
            .as_ref()
            .expect("message accessed after settlement")
            .payload
    }

    fn headers(&self) -> &HeaderMap {
        &self
            .delivery
            .as_ref()
            .expect("message accessed after settlement")
            .headers
    }

    /// The partition key from the `PARTITION_KEY_HEADER`, mirroring the real message so keyed
    /// worker lanes behave the same in-process.
    fn partition_key(&self) -> Option<&[u8]> {
        self.headers().get(crate::PARTITION_KEY_HEADER)
    }

    /// Finalizes the delivery.
    ///
    /// # Errors
    ///
    /// Never fails; the in-process transport has no channel to lose.
    fn ack(mut self) -> impl Future<Output = Result<(), AckError>> {
        drop(self.take());
        ready(Ok(()))
    }

    /// Re-enqueues on the same queue (`requeue = true`) or drops (`requeue = false`).
    ///
    /// A requeued message goes back through the queue rather than to this subscription, so with
    /// competing consumers the redelivery can land on another one, as it does on a server.
    ///
    /// # Errors
    ///
    /// Never fails; the in-process transport has no channel to lose.
    fn nack(mut self, requeue: bool) -> impl Future<Output = Result<(), AckError>> {
        let mut delivery = self.take();
        // The copy that goes back carries the redelivered flag, as a broker would set it.
        delivery.redelivered = requeue;
        if requeue {
            self.state
                .router
                .deliver(&self.queue, delivery, self.coordinator.as_ref());
        }
        ready(Ok(()))
    }
}

impl Partitioned for LapinTestMessage {
    /// The partition key from the `PARTITION_KEY_HEADER`, mirroring the real message.
    fn partition_key(&self) -> Option<&[u8]> {
        self.headers().get(crate::PARTITION_KEY_HEADER)
    }
}
