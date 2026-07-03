//! The in-memory transport core: exact queue-name fanout plus a publish log.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use ruststream::testing::Coordinator;
use ruststream::{Headers, RawMessage};
use tokio::sync::mpsc;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct SubscriptionId(u64);

/// One in-flight test delivery.
#[derive(Debug, Clone)]
pub(crate) struct TestDelivery {
    pub(crate) payload: Bytes,
    pub(crate) headers: Headers,
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
}

/// Routes published messages to subscribers by exact queue name (the default-exchange model);
/// there is no binding or pattern matching here by design.
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

    /// Fans `payload` out to every subscriber of `queue`, synchronously, and appends the message
    /// to the publish log. Every successful enqueue is reported to the coordinator so the
    /// harness's in-flight accounting stays balanced.
    pub(crate) fn publish(
        &self,
        queue: &str,
        payload: &Bytes,
        headers: &Headers,
        coordinator: Option<&Coordinator>,
    ) {
        let senders: Vec<DeliverySender> = {
            let mut state = self.state.lock().expect("test router mutex poisoned");
            state
                .log
                .entry(queue.to_owned())
                .or_default()
                .push(RawMessage::new(queue, payload.clone()).with_headers(headers.clone()));
            state
                .subscriptions
                .values()
                .filter(|subscription| subscription.queue == queue)
                .map(|subscription| subscription.sender.clone())
                .collect()
        };
        for sender in senders {
            let delivery = TestDelivery {
                payload: payload.clone(),
                headers: headers.clone(),
            };
            if sender.send(delivery).is_ok()
                && let Some(coordinator) = coordinator
            {
                coordinator.enqueued();
            }
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
    }
}

impl std::fmt::Debug for KeyRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyRouter").finish_non_exhaustive()
    }
}
