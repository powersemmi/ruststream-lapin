//! The in-process subscriber and its delivery type.

use std::future::{Future, ready};
use std::sync::Arc;

use futures::Stream;
use ruststream::testing::Coordinator;
use ruststream::{AckError, HeaderMap, IncomingMessage, Partitioned, Subscriber};

use super::broker::TestBrokerState;
use super::router::{DeliveryReceiver, DeliverySender, SubscriptionId, TestDelivery};
use crate::error::AmqpError;

/// In-process subscriber on one queue name.
///
/// Yielded messages settle like the real transport: ack finalizes, `nack(true)` re-enqueues to
/// this same subscription, `nack(false)` drops.
pub struct LapinTestSubscriber {
    state: Arc<TestBrokerState>,
    id: SubscriptionId,
    queue: String,
    sender: DeliverySender,
    receiver: DeliveryReceiver,
    coordinator: Option<Coordinator>,
    /// The next channel-local delivery tag. AMQP numbers deliveries per channel from 1, and a
    /// requeued message is handed out under a new tag.
    next_tag: u64,
}

impl LapinTestSubscriber {
    pub(crate) fn open(state: &Arc<TestBrokerState>, queue: String) -> Self {
        let (id, sender, receiver) = state.router.subscribe(queue.clone());
        let coordinator = state.coordinator();
        Self {
            state: Arc::clone(state),
            id,
            queue,
            sender,
            receiver,
            coordinator,
            next_tag: 1,
        }
    }

    /// The queue this subscriber consumes from.
    #[must_use]
    pub fn queue(&self) -> &str {
        &self.queue
    }
}

impl Drop for LapinTestSubscriber {
    fn drop(&mut self) {
        self.state.router.unsubscribe(self.id);
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
        let Self {
            receiver,
            sender,
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
                        sender: sender.clone(),
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
    sender: DeliverySender,
    coordinator: Option<Coordinator>,
}

impl LapinTestMessage {
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

    /// Re-enqueues to the same subscription (`requeue = true`) or drops (`requeue = false`).
    ///
    /// # Errors
    ///
    /// Never fails; the in-process transport has no channel to lose.
    fn nack(mut self, requeue: bool) -> impl Future<Output = Result<(), AckError>> {
        let mut delivery = self.take();
        // The copy that goes back carries the redelivered flag, as a broker would set it.
        delivery.redelivered = requeue;
        if requeue && self.sender.send(delivery).is_ok() {
            // This bypasses the router fanout, so account for the new in-flight delivery here.
            if let Some(coordinator) = &self.coordinator {
                coordinator.enqueued();
            }
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
