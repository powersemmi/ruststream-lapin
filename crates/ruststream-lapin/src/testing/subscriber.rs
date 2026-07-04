//! The in-process subscriber and its delivery type.

use std::sync::Arc;

use futures::Stream;
use ruststream::testing::Coordinator;
use ruststream::{AckError, Headers, IncomingMessage, Partitioned, Subscriber};

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
            ..
        } = self;
        futures::stream::poll_fn(move |cx| {
            receiver.poll_recv(cx).map(|delivery| {
                delivery.map(|delivery| {
                    Ok(LapinTestMessage {
                        delivery: Some(delivery),
                        sender: sender.clone(),
                        coordinator: coordinator.clone(),
                    })
                })
            })
        })
    }
}

/// One in-process delivery.
pub struct LapinTestMessage {
    delivery: Option<TestDelivery>,
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

    fn headers(&self) -> &Headers {
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
    async fn ack(mut self) -> Result<(), AckError> {
        drop(self.take());
        Ok(())
    }

    /// Re-enqueues to the same subscription (`requeue = true`) or drops (`requeue = false`).
    ///
    /// # Errors
    ///
    /// Never fails; the in-process transport has no channel to lose.
    async fn nack(mut self, requeue: bool) -> Result<(), AckError> {
        let delivery = self.take();
        if requeue && self.sender.send(delivery).is_ok() {
            // This bypasses the router fanout, so account for the new in-flight delivery here.
            if let Some(coordinator) = &self.coordinator {
                coordinator.enqueued();
            }
        }
        Ok(())
    }
}

impl Partitioned for LapinTestMessage {
    /// The partition key from the `PARTITION_KEY_HEADER`, mirroring the real message.
    fn partition_key(&self) -> Option<&[u8]> {
        self.headers().get(crate::PARTITION_KEY_HEADER)
    }
}
