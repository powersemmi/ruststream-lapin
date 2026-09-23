//! The consuming half of the in-process transport: the stream of one consumer, and how a delivery
//! taken from it settles.

use std::sync::Arc;
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
                    arguments
                        .inner()
                        .get(&ShortString::from(DELIVERY_LIMIT))
                        .and_then(convert::counter)
                })
                .flatten(),
            dead_letter: argument(arguments, DEAD_LETTER_EXCHANGE)
                .map(|exchange| (exchange, argument(arguments, DEAD_LETTER_ROUTING_KEY))),
        }
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
}

impl BusDeliveries {
    pub(crate) fn open(bus: &Arc<Bus>, queue: String, behaviour: QueueBehaviour) -> Self {
        let (id, receiver) = bus.consume(queue.clone());
        Self {
            bus: Arc::clone(bus),
            id,
            queue,
            receiver,
            next_tag: 1,
            behaviour: Arc::new(behaviour),
        }
    }
}

impl Drop for BusDeliveries {
    fn drop(&mut self) {
        self.bus.cancel(self.id);
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
            ..
        } = self;
        futures::stream::poll_fn(move |cx| {
            receiver.poll_recv(cx).map(|delivery| {
                delivery.map(|delivery| {
                    let tag = *next_tag;
                    *next_tag += 1;
                    Ok(LapinMessage::in_process(
                        delivery,
                        tag,
                        Settlement {
                            bus: Arc::clone(bus),
                            queue: queue.clone(),
                            behaviour: Arc::clone(behaviour),
                            coordinator: bus.coordinator(),
                            no_ack: false,
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
    /// A reply of request/reply arrives on a no-ack consumer, so settling it does nothing.
    no_ack: bool,
}

impl Settlement {
    /// The settlement of a reply, which a no-ack consumer took: there is nothing to settle.
    pub(crate) fn no_ack(bus: &Arc<Bus>, queue: String) -> Self {
        Self {
            bus: Arc::clone(bus),
            queue,
            behaviour: Arc::new(QueueBehaviour::default()),
            coordinator: bus.coordinator(),
            no_ack: true,
        }
    }

    /// Whether a delayed redelivery is the queue's own.
    pub(crate) fn delays(&self) -> bool {
        self.behaviour.delays
    }

    /// `Ok` while the connection that delivered this is open. A live delivery settles on its
    /// channel, and a closed channel refuses the frame.
    fn open(&self, what: &str) -> Result<(), AckError> {
        if self.bus.is_closed() {
            return Err(AckError::Broker(
                format!("{what} was not sent: the connection is closed").into(),
            ));
        }
        Ok(())
    }

    /// `basic.ack`.
    pub(crate) fn ack(&self) -> Result<(), AckError> {
        if self.no_ack {
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
        if self.no_ack {
            return Ok(());
        }
        self.open("basic.reject")?;
        if !requeue {
            self.dead_letter(&delivery);
            return Ok(());
        }
        if self.behaviour.counts {
            let spent = delivery.returns.unwrap_or(0).saturating_add(1);
            if self
                .behaviour
                .delivery_limit
                .is_some_and(|limit| spent > limit)
            {
                self.dead_letter(&delivery);
                return Ok(());
            }
            delivery.returns = Some(spent);
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
        if !self.behaviour.delays || self.no_ack {
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
            None => {
                tokio::spawn(async move {
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
        let Some((exchange, routing_key)) = &self.behaviour.dead_letter else {
            return;
        };
        let routing_key = routing_key.as_deref().unwrap_or(&delivery.routing_key);
        self.bus.reroute(exchange, routing_key, delivery);
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
