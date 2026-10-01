//! The consuming half of the in-process transport: the stream of one consumer, and how a delivery
//! taken from it settles.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use futures::Stream;
use lapin::types::{AMQPValue, FieldTable, ShortString};
use ruststream::testing::Coordinator;
use ruststream::{AckError, Subscriber};

use super::bus::{Bus, BusDelivery, ConsumerId, DeliveryReceiver};
use crate::convert;
use crate::delay::counted_again;
use crate::error::AmqpError;
use crate::message::LapinMessage;
use crate::queue::{
    DEAD_LETTER_EXCHANGE, DEAD_LETTER_ROUTING_KEY, DELIVERY_LIMIT, QueueKind, QueueSpec,
};

/// The `x-delivery-limit` `RabbitMQ` 4 gives a quorum queue declared without one.
const QUORUM_DEFAULT_DELIVERY_LIMIT: u64 = 20;

/// What a queue does with the deliveries it hands out, read off the arguments its declaration
/// produced the way a server reads them off the queue.
#[derive(Debug, Clone, Default)]
pub(crate) struct QueueBehaviour {
    /// The descriptor named a waiting queue, so a delayed redelivery is the broker's rather than
    /// the runtime's deferred copy.
    pub(crate) delays: bool,
    /// The queue counts the deliveries a message has spent, as a quorum queue does and a classic
    /// queue never does.
    pub(crate) counts: bool,
    /// How often the queue returns a message before it carries it away (`x-delivery-limit`).
    pub(crate) delivery_limit: Option<u64>,
    /// Where a rejected or spent message goes: the dead-letter exchange, and the routing key it
    /// carries there when the queue names one instead of the key it arrived with.
    pub(crate) dead_letter: Option<(String, Option<String>)>,
}

impl QueueBehaviour {
    pub(crate) fn of(spec: &QueueSpec, arguments: &FieldTable) -> Self {
        let counts = spec.kind == QueueKind::Quorum;
        Self {
            delays: spec.delay.is_some(),
            counts,
            delivery_limit: counts
                .then(|| {
                    // A quorum queue declared without the argument takes the server's default
                    // limit; a negative one is RabbitMQ's spelling of "unlimited".
                    arguments
                        .inner()
                        .get(&ShortString::from(DELIVERY_LIMIT))
                        .map_or(Some(QUORUM_DEFAULT_DELIVERY_LIMIT), convert::counter)
                })
                .flatten(),
            dead_letter: argument(arguments, DEAD_LETTER_EXCHANGE)
                .map(|exchange| (exchange, argument(arguments, DEAD_LETTER_ROUTING_KEY))),
        }
    }
}

impl QueueBehaviour {
    /// Spends one of `delivery`'s deliveries as it returns to the queue, on a queue that counts
    /// them: whether it has now spent more than the delivery limit, and so takes the dead-letter
    /// route instead of coming back.
    pub(crate) fn spend(&self, delivery: &mut BusDelivery) -> bool {
        if !self.counts {
            return false;
        }
        let spent = delivery.returns.unwrap_or(0).saturating_add(1);
        if self.delivery_limit.is_some_and(|limit| spent > limit) {
            return true;
        }
        delivery.returns = Some(spent);
        false
    }

    /// Routes `delivery` the way the queue dead-letters, when it names a route.
    pub(crate) fn dead_letter(&self, bus: &Bus, delivery: &BusDelivery) {
        let Some((exchange, routing_key)) = &self.dead_letter else {
            return;
        };
        let routing_key = routing_key.as_deref().unwrap_or(&delivery.routing_key);
        bus.reroute(exchange, routing_key, delivery);
    }
}

/// One text argument of a declaration.
fn argument(arguments: &FieldTable, name: &str) -> Option<String> {
    match arguments.inner().get(&ShortString::from(name))? {
        AMQPValue::LongString(value) => Some(value.to_string()),
        AMQPValue::ShortString(value) => Some(value.to_string()),
        _ => None,
    }
}

/// One consumer of a queue on the in-process transport, one delivery at a time: what the
/// subscriber batches over, as it batches over a live consumer.
pub(crate) struct BusDeliveries {
    bus: Arc<Bus>,
    id: ConsumerId,
    queue: String,
    receiver: DeliveryReceiver,
    /// The next delivery tag. A channel numbers its deliveries from 1, and a requeued message is
    /// handed out under a new tag.
    next_tag: u64,
    behaviour: Arc<QueueBehaviour>,
    /// What this consumer handed out and nobody settled yet, which the server returns to the
    /// queue when the consumer's channel closes.
    unsettled: Arc<Unsettled>,
}

/// The deliveries one consumer handed out and nobody has settled, by delivery tag.
///
/// `None` once the consumer has closed: what it held went back to the queue then, and a
/// settlement arriving later finds its channel gone.
#[derive(Debug, Default)]
pub(crate) struct Unsettled(Mutex<Option<BTreeMap<u64, BusDelivery>>>);

impl Unsettled {
    fn open() -> Self {
        Self(Mutex::new(Some(BTreeMap::new())))
    }

    fn lock(&self) -> MutexGuard<'_, Option<BTreeMap<u64, BusDelivery>>> {
        self.0
            .lock()
            .expect("in-process unsettled deliveries mutex poisoned")
    }

    fn hand_out(&self, tag: u64, delivery: BusDelivery) {
        if let Some(held) = self.lock().as_mut() {
            held.insert(tag, delivery);
        }
    }

    /// Takes `tag` off the consumer as its settlement: `false` when the consumer has closed and
    /// the server has already returned the delivery to the queue.
    fn settle(&self, tag: u64) -> bool {
        self.lock()
            .as_mut()
            .is_some_and(|held| held.remove(&tag).is_some())
    }

    /// Closes the consumer, handing back what it held, in delivery order.
    fn close(&self) -> Vec<BusDelivery> {
        self.lock()
            .take()
            .map(|held| held.into_values().collect())
            .unwrap_or_default()
    }
}

impl BusDeliveries {
    pub(crate) fn open(
        bus: &Arc<Bus>,
        queue: String,
        auto_delete: bool,
        behaviour: QueueBehaviour,
    ) -> Self {
        let (id, receiver) = bus.consume_queue(&queue, auto_delete);
        Self {
            bus: Arc::clone(bus),
            id,
            queue,
            receiver,
            next_tag: 1,
            behaviour: Arc::new(behaviour),
            unsettled: Arc::new(Unsettled::open()),
        }
    }
}

impl Drop for BusDeliveries {
    fn drop(&mut self) {
        self.bus.cancel(self.id);
        // Handed out and left unsettled, then handed to this consumer and never read: back to
        // the queue in that order, as the server requeues what a closing consumer had not
        // acknowledged.
        for delivery in self.unsettled.close() {
            self.bus
                .requeue_unsettled(&self.queue, delivery, &self.behaviour);
        }
        self.receiver.close();
        while let Ok(delivery) = self.receiver.try_recv() {
            self.bus
                .requeue_unread(&self.queue, delivery, &self.behaviour);
        }
    }
}

impl std::fmt::Debug for BusDeliveries {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BusDeliveries")
            .field("queue", &self.queue)
            .finish_non_exhaustive()
    }
}

impl Subscriber for BusDeliveries {
    type Message = LapinMessage;
    type Error = AmqpError;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        let Self {
            bus,
            receiver,
            queue,
            next_tag,
            behaviour,
            unsettled,
            ..
        } = self;
        futures::stream::poll_fn(move |cx| {
            receiver.poll_recv(cx).map(|delivery| {
                delivery.map(|delivery| {
                    let tag = *next_tag;
                    *next_tag += 1;
                    unsettled.hand_out(tag, delivery.clone());
                    Ok(LapinMessage::in_process(
                        delivery,
                        tag,
                        Settlement {
                            bus: Arc::clone(bus),
                            queue: queue.clone(),
                            behaviour: Arc::clone(behaviour),
                            coordinator: bus.coordinator(),
                            held: Some((Arc::clone(unsettled), tag)),
                        },
                    ))
                })
            })
        })
    }
}

/// How an in-process delivery settles: against the queue it came from, with what that queue does
/// with a returned or rejected message.
///
/// Releases the delivery to the harness once, when it goes, whichever way it settled.
pub(crate) struct Settlement {
    bus: Arc<Bus>,
    queue: String,
    behaviour: Arc<QueueBehaviour>,
    coordinator: Option<Coordinator>,
    /// The consumer that handed the delivery out, and its tag there; `None` for a reply of
    /// request/reply, which arrives on a no-ack consumer, so settling it does nothing.
    held: Option<(Arc<Unsettled>, u64)>,
}

impl Settlement {
    /// The settlement of a reply, which a no-ack consumer took: there is nothing to settle.
    pub(crate) fn of_reply(bus: &Arc<Bus>, queue: String) -> Self {
        Self {
            bus: Arc::clone(bus),
            queue,
            behaviour: Arc::new(QueueBehaviour::default()),
            coordinator: bus.coordinator(),
            held: None,
        }
    }

    /// Whether this delivery came from a no-ack consumer, where a settlement does nothing.
    const fn no_ack(&self) -> bool {
        self.held.is_none()
    }

    /// Whether a delayed redelivery is the queue's own.
    pub(crate) fn delays(&self) -> bool {
        self.behaviour.delays
    }

    /// `Ok` while the connection that delivered this is open. A live delivery settles on its
    /// channel, and a closed channel refuses the frame.
    ///
    /// It takes the delivery off its consumer too: a consumer that has closed returned it to the
    /// queue already, and its channel refuses the frame.
    fn open(&self, what: &str) -> Result<(), AckError> {
        if self.bus.is_closed() {
            return Err(AckError::Broker(
                format!("{what} was not sent: the connection is closed").into(),
            ));
        }
        if let Some((unsettled, tag)) = &self.held
            && !unsettled.settle(*tag)
        {
            return Err(AckError::Broker(
                format!("{what} was not sent: the consumer's channel is closed").into(),
            ));
        }
        Ok(())
    }

    /// `basic.ack`.
    pub(crate) fn ack(&self) -> Result<(), AckError> {
        if self.no_ack() {
            return Ok(());
        }
        self.open("basic.ack")
    }

    /// `basic.reject`, back to the queue under `requeue`, to the dead-letter route otherwise.
    ///
    /// On a queue that counts, a returned message spends one of its deliveries, and once it has
    /// spent more than the queue's delivery limit it takes the dead-letter route instead of
    /// coming back.
    pub(crate) fn reject(&self, mut delivery: BusDelivery, requeue: bool) -> Result<(), AckError> {
        if self.no_ack() {
            return Ok(());
        }
        self.open("basic.reject")?;
        if !requeue {
            self.dead_letter(&delivery);
            return Ok(());
        }
        if self.behaviour.spend(&mut delivery) {
            self.dead_letter(&delivery);
            return Ok(());
        }
        delivery.redelivered = true;
        self.bus.deliver(&self.queue, delivery);
        Ok(())
    }

    /// The native delayed redelivery through the waiting queue: the copy leaves now, carrying the
    /// framework's retry count raised by one as the live re-publish does, and returns to the queue
    /// once `delay` has passed; the original is acknowledged.
    ///
    /// # Errors
    ///
    /// Returns [`AckError::Unsupported`] when the descriptor named no waiting queue, which is what
    /// tells the runtime to publish its own deferred copy.
    pub(crate) fn nack_after(
        &self,
        delivery: BusDelivery,
        delay: Duration,
    ) -> Result<(), AckError> {
        if !self.behaviour.delays || self.no_ack() {
            return Err(AckError::Unsupported);
        }
        self.open("basic.ack")?;
        let options = convert::redelivery_options(&delivery.headers);
        let headers = convert::properties_for_publish(&counted_again(&delivery.headers), &options)
            .map(|properties| convert::headers_from_properties(&properties))
            .map_err(|err| AckError::Broker(Box::new(err)))?;
        // The waiting queue dead-letters the copy back through the default exchange under the
        // queue's own name, and a message that arrives that way is a new one.
        let copy = BusDelivery {
            headers,
            exchange: String::new(),
            routing_key: self.queue.clone(),
            redelivered: false,
            returns: None,
            ..delivery
        };
        let bus = Arc::clone(&self.bus);
        let queue = self.queue.clone();
        let redeliver = move || bus.deliver(&queue, copy);
        match self.coordinator.as_ref() {
            // Under the harness the timer belongs to the coordinator, so `TestApp::advance` fires
            // it deterministically instead of the test waiting on a wall clock.
            Some(coordinator) => coordinator.schedule_redelivery(delay, redeliver),
            // On the runtime the broker connected on, not the settling caller's: a handler on a
            // dedicated thread settles from a runtime that may stop before the delay is out.
            None => {
                self.bus.runtime().spawn(async move {
                    tokio::time::sleep(delay).await;
                    redeliver();
                });
            }
        }
        Ok(())
    }

    /// Sends a rejected or spent delivery where the queue's dead-letter route sends it, or lets it
    /// go when the queue names none, as a server does.
    fn dead_letter(&self, delivery: &BusDelivery) {
        self.behaviour.dead_letter(&self.bus, delivery);
    }
}

impl Drop for Settlement {
    fn drop(&mut self) {
        // Balances the transport's `enqueued` once per delivery, whatever the dispatch path did
        // (ack, nack, panic, or a plain drop).
        if let Some(coordinator) = self.coordinator.take() {
            coordinator.consumed();
        }
    }
}

impl std::fmt::Debug for Settlement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Settlement")
            .field("queue", &self.queue)
            .finish_non_exhaustive()
    }
}
