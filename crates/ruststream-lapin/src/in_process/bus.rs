//! The in-process transport's state: the queues with their consumers, the exchanges and bindings
//! the service's descriptors describe, what was declared, and the publish log.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

use bytes::Bytes;
use lapin::types::{FieldTable, ShortString};
use lapin::{BasicProperties, ExchangeKind};
use ruststream::testing::Coordinator;
use ruststream::{HeaderMap, RawMessage};
use tokio::runtime::Handle;
use tokio::sync::mpsc;

use super::deliveries::QueueBehaviour;
use super::matching::{headers_match, topic_matches};
use crate::convert;
use crate::error::AmqpError;
use crate::publish_step::{EXPIRATION_HEADER, LapinPublishOptions, PRIORITY_HEADER};
use crate::queue::{QueueKind, QueueSpec};

/// The largest message body a server accepts: `RabbitMQ`'s `max_message_size`, 16 MiB unless the
/// operator changed it. A publish over it closes the channel, and a confirming publisher reports
/// the refusal.
const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

/// The one queue argument a quorum queue refuses outright, which a server answers with
/// `PRECONDITION_FAILED` at declaration.
const MAX_PRIORITY: &str = "x-max-priority";

/// One message on its way to a consumer: what a live delivery reports, read off the same frame
/// conversion a live publish goes through.
#[derive(Debug, Clone)]
pub(crate) struct BusDelivery {
    pub(crate) payload: Bytes,
    /// The headers a consumer reads: the header table and the native properties, as a live
    /// delivery reports them.
    pub(crate) headers: HeaderMap,
    /// The exchange the message was published to, empty for the default exchange.
    pub(crate) exchange: String,
    pub(crate) routing_key: String,
    /// Set on the copy a `nack(requeue = true)` puts back, as the server sets it.
    pub(crate) redelivered: bool,
    /// How often a quorum queue has returned this message, which it stamps as `x-delivery-count`;
    /// `None` until it has come back once, and always on a classic queue.
    pub(crate) returns: Option<u64>,
}

pub(crate) type DeliverySender = mpsc::UnboundedSender<BusDelivery>;
pub(crate) type DeliveryReceiver = mpsc::UnboundedReceiver<BusDelivery>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct ConsumerId(u64);

#[derive(Debug)]
struct Consumer {
    queue: String,
    sender: DeliverySender,
}

/// One binding a descriptor describes: what routes into `queue` from `exchange`.
#[derive(Debug, Clone)]
struct Bound {
    exchange: String,
    kind: ExchangeKind,
    routing_key: String,
    arguments: FieldTable,
    queue: String,
}

/// What a declaration asked a queue to be, which a second declaration has to repeat.
#[derive(Debug, Clone, PartialEq)]
struct DeclaredQueue {
    kind: QueueKind,
    durable: bool,
    exclusive: bool,
    auto_delete: bool,
    arguments: FieldTable,
}

/// What a declaration asked an exchange to be.
#[derive(Debug, Clone, PartialEq, Eq)]
struct DeclaredExchange {
    kind: ExchangeKind,
    durable: bool,
    auto_delete: bool,
}

#[derive(Debug, Default)]
struct Routes {
    // Ordered by id, so the rotation below is reproducible: a test that opens two consumers of
    // one queue always sees the same one served first.
    consumers: BTreeMap<ConsumerId, Consumer>,
    bindings: Vec<Bound>,
    queues: HashMap<String, DeclaredQueue>,
    exchanges: HashMap<String, DeclaredExchange>,
    log: HashMap<String, Vec<RawMessage>>,
    /// How many deliveries each queue has handed out, which is what makes the next consumer
    /// choice a rotation rather than a coin flip.
    dispatched: HashMap<String, usize>,
    /// The queues a subscription has opened, with what reached each while it had no consumer.
    stored: HashMap<String, Stored>,
}

/// A queue that exists on the server, and the messages it keeps while no consumer takes them.
#[derive(Debug, Default)]
struct Stored {
    /// The server deletes the queue, and what it keeps, once its last consumer goes.
    auto_delete: bool,
    waiting: VecDeque<BusDelivery>,
}

impl Routes {
    /// Picks the consumer of `queue` the next delivery goes to and advances the rotation.
    fn next_consumer(&mut self, queue: &str) -> Option<DeliverySender> {
        let consumers: Vec<&Consumer> = self
            .consumers
            .values()
            .filter(|consumer| consumer.queue == queue)
            .collect();
        if consumers.is_empty() {
            return None;
        }
        let turn = self.dispatched.entry(queue.to_owned()).or_default();
        let index = *turn % consumers.len();
        *turn = turn.wrapping_add(1);
        Some(consumers[index].sender.clone())
    }

    /// The consumer of `queue` that `delivery` goes to, with the delivery; `None` when the queue
    /// has no consumer, having kept the delivery if the queue exists.
    fn hand_over(
        &mut self,
        queue: &str,
        delivery: BusDelivery,
    ) -> Option<(DeliverySender, BusDelivery)> {
        if let Some(sender) = self.next_consumer(queue) {
            return Some((sender, delivery));
        }
        if let Some(stored) = self.stored.get_mut(queue) {
            stored.waiting.push_back(delivery);
        }
        None
    }

    /// Which of `queue`'s consumers the next delivery goes to, as an index into them in the order
    /// they opened, without advancing the rotation; `None` when the queue has no consumer.
    fn next_turn(&self, queue: &str) -> Option<usize> {
        let consumers = self
            .consumers
            .values()
            .filter(|consumer| consumer.queue == queue)
            .count();
        if consumers == 0 {
            return None;
        }
        Some(self.dispatched.get(queue).copied().unwrap_or_default() % consumers)
    }

    /// The queues a message published to `exchange` under `routing_key` reaches, each once.
    ///
    /// The default exchange routes to the queue of the routing key's name. Every other exchange
    /// routes through the bindings the service's descriptors describe, by its kind's rule; a
    /// plugin exchange routes nothing here.
    fn targets(
        &self,
        exchange: &str,
        routing_key: &str,
        table: Option<&FieldTable>,
    ) -> Vec<String> {
        if exchange.is_empty() {
            return vec![routing_key.to_owned()];
        }
        let mut queues: Vec<String> = Vec::new();
        for bound in self
            .bindings
            .iter()
            .filter(|bound| bound.exchange == exchange)
        {
            let routed = match &bound.kind {
                ExchangeKind::Direct => bound.routing_key == routing_key,
                ExchangeKind::Topic => topic_matches(&bound.routing_key, routing_key),
                ExchangeKind::Fanout => true,
                ExchangeKind::Headers => headers_match(&bound.arguments, table),
                ExchangeKind::Custom(_) => false,
            };
            if routed && !queues.contains(&bound.queue) {
                queues.push(bound.queue.clone());
            }
        }
        queues
    }
}

/// The transport behind one in-process connection, shared by the connected broker and every
/// subscriber, publisher and delivery taken from it.
pub(crate) struct Bus {
    routes: Mutex<Routes>,
    next_consumer: AtomicU64,
    /// Mirrors a closed connection: handles that outlive the shutdown report an error rather than
    /// route into a dead transport.
    closed: AtomicBool,
    coordinator: OnceLock<Coordinator>,
    /// Numbers the private reply addresses of request/reply. The transport owns the sequence,
    /// like the server that rewrites the direct reply-to address, so two requesters on one
    /// connection are never handed the same address.
    inbox_seq: AtomicU64,
    /// The runtime the broker connected on, which a delayed redelivery waits on.
    runtime: Handle,
}

impl Bus {
    pub(crate) fn new(runtime: Handle) -> Arc<Self> {
        Arc::new(Self {
            routes: Mutex::new(Routes::default()),
            next_consumer: AtomicU64::new(0),
            closed: AtomicBool::new(false),
            coordinator: OnceLock::new(),
            inbox_seq: AtomicU64::new(0),
            runtime,
        })
    }

    /// The runtime the broker connected on: a task the transport starts on its own behalf runs
    /// there, whichever thread settles the delivery that asked for it.
    pub(crate) const fn runtime(&self) -> &Handle {
        &self.runtime
    }

    fn routes(&self) -> MutexGuard<'_, Routes> {
        self.routes
            .lock()
            .expect("in-process routes mutex poisoned")
    }

    /// Installs the harness coordinator. A second install is ignored: the harness contract asks
    /// for idempotency.
    pub(crate) fn install(&self, coordinator: Coordinator) {
        let _ = self.coordinator.set(coordinator);
    }

    pub(crate) fn coordinator(&self) -> Option<Coordinator> {
        self.coordinator.get().cloned()
    }

    pub(crate) fn next_inbox(&self) -> u64 {
        self.inbox_seq.fetch_add(1, Ordering::Relaxed)
    }

    /// `Ok` while the connection is live, [`AmqpError::Closed`] once it has shut down.
    pub(crate) fn ensure_live(&self, target: &str) -> Result<(), AmqpError> {
        if self.is_closed() {
            return Err(AmqpError::closed(target));
        }
        Ok(())
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// Closes the connection: every consumer's stream ends, and every handle reports it closed.
    pub(crate) fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.routes().consumers.clear();
    }

    /// Opens a consumer of the queue a subscription opened: the queue exists from now on, and
    /// what it kept while it had no consumer is delivered first, in the order it arrived.
    pub(crate) fn consume_queue(
        &self,
        queue: &str,
        auto_delete: bool,
    ) -> (ConsumerId, DeliveryReceiver) {
        // One guard over the registration and the drain: a publish that lands in between would
        // reach the new consumer ahead of the messages the queue kept for it.
        let mut routes = self.routes();
        let opened = self.register(&mut routes, queue.to_owned());
        let waiting = std::mem::take(
            &mut routes
                .stored
                .entry(queue.to_owned())
                .or_insert(Stored {
                    auto_delete,
                    waiting: VecDeque::new(),
                })
                .waiting,
        );
        for delivery in waiting {
            self.hand_over(&mut routes, queue, delivery);
        }
        drop(routes);
        opened
    }

    /// Opens a consumer of `queue`.
    pub(crate) fn consume(&self, queue: String) -> (ConsumerId, DeliveryReceiver) {
        self.register(&mut self.routes(), queue)
    }

    fn register(&self, routes: &mut Routes, queue: String) -> (ConsumerId, DeliveryReceiver) {
        let id = ConsumerId(self.next_consumer.fetch_add(1, Ordering::Relaxed));
        let (sender, receiver) = mpsc::unbounded_channel();
        routes.consumers.insert(id, Consumer { queue, sender });
        (id, receiver)
    }

    /// Cancels a consumer. An auto-delete queue goes with its last consumer, and what it kept
    /// goes with it.
    pub(crate) fn cancel(&self, id: ConsumerId) {
        let mut routes = self.routes();
        let Some(consumer) = routes.consumers.remove(&id) else {
            return;
        };
        let last = !routes
            .consumers
            .values()
            .any(|other| other.queue == consumer.queue);
        if last
            && routes
                .stored
                .get(&consumer.queue)
                .is_some_and(|stored| stored.auto_delete)
        {
            routes.stored.remove(&consumer.queue);
        }
    }

    /// Records what the descriptor of a subscription describes: its bindings always, since
    /// routing through an exchange needs them, and its declaration when the broker declares
    /// topology, refused where a server refuses it.
    ///
    /// # Errors
    ///
    /// Returns [`AmqpError::Declare`] for what a server refuses to declare: a transient queue that
    /// is not exclusive, a binding on the default exchange, an argument the queue's type does not
    /// take, or a queue or exchange declared again with other settings.
    pub(crate) fn describe(
        &self,
        spec: &QueueSpec,
        arguments: &FieldTable,
        declares: bool,
    ) -> Result<(), AmqpError> {
        let mut routes = self.routes();
        if declares {
            declare(&mut routes, spec, arguments)?;
        }
        for binding in &spec.bindings {
            let exchange = binding.exchange.name();
            let known = routes.bindings.iter().any(|bound| {
                bound.exchange == exchange
                    && bound.routing_key == binding.routing_key
                    && bound.queue == spec.name
                    && bound.arguments == binding.arguments
            });
            if !known {
                routes.bindings.push(Bound {
                    exchange: exchange.to_owned(),
                    kind: binding.exchange.kind().clone(),
                    routing_key: binding.routing_key.clone(),
                    arguments: binding.arguments.clone(),
                    queue: spec.name.clone(),
                });
            }
        }
        drop(routes);
        Ok(())
    }

    /// Publishes a message the way a live publisher frames it: the names are checked as the
    /// protocol checks them, the headers and `options` become the frame's properties, and the
    /// consumer reads them back from those properties as it reads a live delivery.
    ///
    /// The publish log records the message as the publisher was handed it, under its routing
    /// key.
    ///
    /// # Errors
    ///
    /// Returns [`AmqpError::Closed`] once the connection has shut down,
    /// [`AmqpError::InvalidOptions`] for a name or a header the protocol cannot carry, and
    /// [`AmqpError::Publish`] for a body over the server's `max_message_size`.
    pub(crate) fn publish(
        &self,
        exchange: &str,
        routing_key: &str,
        payload: &[u8],
        headers: &HeaderMap,
        options: &LapinPublishOptions,
    ) -> Result<(), AmqpError> {
        let properties = self.check(exchange, routing_key, headers, options)?;
        if payload.len() > MAX_MESSAGE_SIZE {
            return Err(AmqpError::Publish(
                format!(
                    "a message body of {} bytes to {routing_key:?} is over the server's \
                     max_message_size of {MAX_MESSAGE_SIZE} bytes",
                    payload.len()
                )
                .into(),
            ));
        }
        let payload = Bytes::copy_from_slice(payload);
        let queues =
            {
                let mut routes = self.routes();
                routes.log.entry(routing_key.to_owned()).or_default().push(
                    RawMessage::new(routing_key, payload.clone()).with_headers(headers.clone()),
                );
                routes.targets(exchange, routing_key, properties.headers().as_ref())
            };
        let delivery = BusDelivery {
            payload,
            headers: convert::headers_from_properties(&properties),
            exchange: exchange.to_owned(),
            routing_key: routing_key.to_owned(),
            redelivered: false,
            returns: None,
        };
        for queue in queues {
            self.deliver(&queue, delivery.clone());
        }
        Ok(())
    }

    /// Frames a publish without sending it: the checks a live publish makes before the server
    /// takes the message, and the properties it would carry.
    ///
    /// # Errors
    ///
    /// As [`publish`](Self::publish).
    pub(crate) fn check(
        &self,
        exchange: &str,
        routing_key: &str,
        headers: &HeaderMap,
        options: &LapinPublishOptions,
    ) -> Result<BasicProperties, AmqpError> {
        self.ensure_live(routing_key)?;
        convert::short(exchange, "exchange name")?;
        convert::short(routing_key, "routing key")?;
        convert::properties_for_publish(headers, options)
    }

    /// Routes `delivery` through `exchange` under `routing_key` without logging it: how a queue
    /// dead-letters a message, which is the server's move and not a publish of the service.
    pub(crate) fn reroute(&self, exchange: &str, routing_key: &str, delivery: &BusDelivery) {
        // A headers exchange matches the dead-lettered message's own header table. The priority
        // and the expiration a delivery reports as headers are native properties on the server,
        // not entries of that table, so they match no binding.
        let mut own = delivery.headers.clone();
        own.remove(PRIORITY_HEADER);
        own.remove(EXPIRATION_HEADER);
        let properties =
            convert::properties_for_publish(&own, &LapinPublishOptions::default()).ok();
        let table = properties
            .as_ref()
            .and_then(|properties| properties.headers().as_ref());
        let queues = self.routes().targets(exchange, routing_key, table);
        for queue in queues {
            self.deliver(
                &queue,
                BusDelivery {
                    exchange: exchange.to_owned(),
                    routing_key: routing_key.to_owned(),
                    redelivered: false,
                    returns: None,
                    ..delivery.clone()
                },
            );
        }
    }

    /// Hands `delivery` to the next consumer of `queue`. A queue with no consumer keeps it for
    /// the next one; a name no subscription has opened is no queue, and the message goes, as the
    /// default exchange drops what it cannot route.
    ///
    /// A successful hand-over is reported to the coordinator, so the harness's in-flight count
    /// stays balanced against the delivery's release.
    pub(crate) fn deliver(&self, queue: &str, delivery: BusDelivery) {
        self.hand_over(&mut self.routes(), queue, delivery);
    }

    /// [`deliver`](Self::deliver) under a guard the caller holds.
    ///
    /// The send runs under the routes lock. A closing consumer cancels under it and then drains
    /// its channel back to the queue, so a delivery either reaches a consumer that will return it
    /// or is routed past one that is gone; it is never sent into a closed channel. An unbounded
    /// send never blocks, so holding the lock costs no wait.
    fn hand_over(&self, routes: &mut Routes, queue: &str, delivery: BusDelivery) {
        let Some((sender, delivery)) = routes.hand_over(queue, delivery) else {
            return;
        };
        // Counted before the send: a consumer on another task may settle the delivery, and
        // release its count, before `send` returns here.
        let coordinator = self.coordinator.get();
        if let Some(coordinator) = coordinator {
            coordinator.enqueued();
        }
        if sender.send(delivery).is_err()
            && let Some(coordinator) = coordinator
        {
            coordinator.consumed();
        }
    }

    /// Puts back what a cancelled consumer had been handed and never read, as the server
    /// returns a closing consumer's deliveries: the next consumer of the queue receives it,
    /// marked redelivered. A queue that counts deliveries counts this return as one, and a
    /// message past the delivery limit takes the dead-letter route instead. Its count moves
    /// with it.
    pub(crate) fn requeue_unread(
        &self,
        queue: &str,
        delivery: BusDelivery,
        behaviour: &QueueBehaviour,
    ) {
        self.requeue(queue, delivery, behaviour);
        if let Some(coordinator) = self.coordinator.get() {
            coordinator.consumed();
        }
    }

    /// Puts back a delivery a cancelled consumer had handed out and nobody settled, as the server
    /// returns what a closing channel left unacknowledged. The delivery's own settlement already
    /// released it to the harness, so only the redelivery is counted.
    pub(crate) fn requeue_unsettled(
        &self,
        queue: &str,
        delivery: BusDelivery,
        behaviour: &QueueBehaviour,
    ) {
        self.requeue(queue, delivery, behaviour);
    }

    /// Returns `delivery` to `queue`, spending one of its deliveries on a queue that counts them.
    fn requeue(&self, queue: &str, mut delivery: BusDelivery, behaviour: &QueueBehaviour) {
        if behaviour.spend(&mut delivery) {
            behaviour.dead_letter(self, &delivery);
        } else {
            self.deliver(
                queue,
                BusDelivery {
                    redelivered: true,
                    ..delivery
                },
            );
        }
    }

    /// Which of `queue`'s consumers, counted in the order they opened, the next message
    /// published to it on the default exchange reaches: the one the rotation serves next, or
    /// none when the queue has no consumer and the message is dropped.
    pub(crate) fn next_consumer_of(&self, queue: &str) -> Option<usize> {
        self.routes().next_turn(queue)
    }

    /// Every message published under `routing_key`, in publish order.
    pub(crate) fn published(&self, routing_key: &str) -> Vec<RawMessage> {
        self.routes()
            .log
            .get(routing_key)
            .cloned()
            .unwrap_or_default()
    }
}

impl std::fmt::Debug for Bus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Bus")
            .field("closed", &self.is_closed())
            .finish_non_exhaustive()
    }
}

/// Declares what `spec` describes, refusing what a server refuses.
fn declare(routes: &mut Routes, spec: &QueueSpec, arguments: &FieldTable) -> Result<(), AmqpError> {
    for binding in &spec.bindings {
        let exchange = &binding.exchange;
        if exchange.name().is_empty() {
            return Err(refused(format!(
                "queue {:?} binds to the default exchange, and the server permits no operation on \
                 it",
                spec.name
            )));
        }
        convert::short(exchange.name(), "exchange name")?;
        convert::short(&binding.routing_key, "routing key")?;
        // The built-in exchanges exist on every broker and are not declared.
        if exchange.name().starts_with("amq.") {
            continue;
        }
        let declared = DeclaredExchange {
            kind: exchange.kind().clone(),
            durable: exchange.is_durable(),
            auto_delete: exchange.is_auto_delete(),
        };
        match routes.exchanges.get(exchange.name()) {
            Some(existing) if *existing != declared => {
                return Err(refused(format!(
                    "exchange {:?} is declared again with other settings ({declared:?}, where it \
                     was {existing:?})",
                    exchange.name()
                )));
            }
            Some(_) => {}
            None => {
                routes
                    .exchanges
                    .insert(exchange.name().to_owned(), declared);
            }
        }
    }

    convert::short(&spec.name, "queue name")?;
    if !spec.durable && !spec.exclusive {
        return Err(refused(format!(
            "queue {:?} is transient and not exclusive, which RabbitMQ 4 does not permit; declare \
             it durable, or exclusive to the connection",
            spec.name
        )));
    }
    if spec.kind == QueueKind::Quorum
        && arguments
            .inner()
            .contains_key(&ShortString::from(MAX_PRIORITY))
    {
        return Err(refused(format!(
            "quorum queue {:?} is declared with `{MAX_PRIORITY}`, an argument a quorum queue does \
             not take",
            spec.name
        )));
    }
    let declared = DeclaredQueue {
        kind: spec.kind,
        durable: spec.durable,
        exclusive: spec.exclusive,
        auto_delete: spec.auto_delete,
        arguments: arguments.clone(),
    };
    match routes.queues.get(&spec.name) {
        Some(existing) if *existing != declared => Err(refused(format!(
            "queue {:?} is declared again with other settings, which the server refuses as \
             inequivalent",
            spec.name
        ))),
        Some(_) => Ok(()),
        None => {
            routes.queues.insert(spec.name.clone(), declared);
            Ok(())
        }
    }
}

fn refused(reason: String) -> AmqpError {
    AmqpError::Declare(reason.into())
}
