//! The in-memory transport core: exact queue-name fanout plus a publish log.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use ruststream::testing::Coordinator;
use ruststream::{HeaderMap, RawMessage};
use tokio::sync::mpsc;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct SubscriptionId(u64);

/// One in-flight test delivery.
#[derive(Debug, Clone)]
pub(crate) struct TestDelivery {
    pub(crate) payload: Bytes,
    pub(crate) headers: HeaderMap,
    /// Set on the copy a `nack(requeue = true)` puts back, mirroring the flag the broker sets on
    /// a redelivered AMQP delivery.
    pub(crate) redelivered: bool,
}

pub(crate) type DeliverySender = mpsc::UnboundedSender<TestDelivery>;
pub(crate) type DeliveryReceiver = mpsc::UnboundedReceiver<TestDelivery>;

#[derive(Debug)]
struct Subscription {
    queue: String,
    sender: DeliverySender,
}

#[derive(Debug, Default)]
struct RouterState {
    subscriptions: HashMap<SubscriptionId, Subscription>,
    log: HashMap<String, Vec<RawMessage>>,
    /// How many deliveries each queue has handed out, which is what makes the next consumer
    /// choice a rotation rather than a coin flip.
    dispatched: HashMap<String, usize>,
}

impl RouterState {
    /// The consumers of `queue`, in subscription order.
    ///
    /// Ordered by id rather than by hash slot so the rotation below is reproducible across runs:
    /// a test that opens two subscriptions must always see the same one served first.
    fn consumers_of(&self, queue: &str) -> Vec<DeliverySender> {
        let mut consumers: Vec<(SubscriptionId, DeliverySender)> = self
            .subscriptions
            .iter()
            .filter(|(_, subscription)| subscription.queue == queue)
            .map(|(id, subscription)| (*id, subscription.sender.clone()))
            .collect();
        consumers.sort_by_key(|(id, _)| id.0);
        consumers.into_iter().map(|(_, sender)| sender).collect()
    }

    /// Picks the consumer this delivery goes to and advances the rotation.
    fn next_consumer(&mut self, queue: &str) -> Option<DeliverySender> {
        let consumers = self.consumers_of(queue);
        if consumers.is_empty() {
            return None;
        }
        let turn = self.dispatched.entry(queue.to_owned()).or_default();
        let index = *turn % consumers.len();
        *turn = turn.wrapping_add(1);
        consumers.into_iter().nth(index)
    }
}

/// Routes published messages to subscribers by exact queue name (the default-exchange model);
/// there is no binding or pattern matching here by design.
///
/// A queue with several consumers is a work queue, not a fan-out: each delivery goes to exactly
/// one of them, in rotation, the way `RabbitMQ` dispatches to competing consumers. What the
/// rotation does not model is prefetch - a real server skips a consumer that is at its unacked
/// limit, so with slow handlers the two distributions differ.
#[derive(Default)]
pub(crate) struct KeyRouter {
    state: Mutex<RouterState>,
    next_id: AtomicU64,
}

impl KeyRouter {
    pub(crate) fn subscribe(
        &self,
        queue: String,
    ) -> (SubscriptionId, DeliverySender, DeliveryReceiver) {
        let id = SubscriptionId(self.next_id.fetch_add(1, Ordering::Relaxed));
        let (sender, receiver) = mpsc::unbounded_channel();
        self.state
            .lock()
            .expect("test router mutex poisoned")
            .subscriptions
            .insert(
                id,
                Subscription {
                    queue,
                    sender: sender.clone(),
                },
            );
        (id, sender, receiver)
    }

    pub(crate) fn unsubscribe(&self, id: SubscriptionId) {
        let mut state = self.state.lock().expect("test router mutex poisoned");
        state.subscriptions.remove(&id);
    }

    /// Delivers `payload` to one consumer of `queue`, synchronously, and appends the message to
    /// the publish log.
    pub(crate) fn publish(
        &self,
        queue: &str,
        payload: &Bytes,
        headers: &HeaderMap,
        coordinator: Option<&Coordinator>,
    ) {
        self.state
            .lock()
            .expect("test router mutex poisoned")
            .log
            .entry(queue.to_owned())
            .or_default()
            .push(RawMessage::new(queue, payload.clone()).with_headers(headers.clone()));
        self.deliver(
            queue,
            TestDelivery {
                payload: payload.clone(),
                headers: headers.clone(),
                redelivered: false,
            },
            coordinator,
        );
    }

    /// Hands `delivery` to the queue's next consumer, or drops it when the queue has none - the
    /// unroutable message of the default exchange.
    ///
    /// The publish log is not touched: a requeue takes this path too, and a redelivery is not a
    /// second publish. A successful enqueue is reported to the coordinator so the harness's
    /// in-flight accounting stays balanced.
    pub(crate) fn deliver(
        &self,
        queue: &str,
        delivery: TestDelivery,
        coordinator: Option<&Coordinator>,
    ) {
        let sender = self
            .state
            .lock()
            .expect("test router mutex poisoned")
            .next_consumer(queue);
        if let Some(sender) = sender
            && sender.send(delivery).is_ok()
            && let Some(coordinator) = coordinator
        {
            coordinator.enqueued();
        }
    }

    pub(crate) fn published(&self, queue: &str) -> Vec<RawMessage> {
        let state = self.state.lock().expect("test router mutex poisoned");
        state.log.get(queue).cloned().unwrap_or_default()
    }

    pub(crate) fn clear(&self) {
        let mut state = self.state.lock().expect("test router mutex poisoned");
        state.subscriptions.clear();
        state.log.clear();
        state.dispatched.clear();
    }
}

impl std::fmt::Debug for KeyRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyRouter").finish_non_exhaustive()
    }
}
