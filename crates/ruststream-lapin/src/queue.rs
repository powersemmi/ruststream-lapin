//! The queue descriptor: what a subscription binds to and, optionally, expects to exist.

use std::future::{Future, ready};
use std::num::NonZeroU16;
use std::time::Duration;

use lapin::types::{AMQPValue, FieldTable, ShortString};
use ruststream::runtime::IntoSource;
use ruststream::{RedeliveryAddress, SubscriptionSource};

use crate::broker::ConnectedLapinBroker;
use crate::delay::Delay;
use crate::error::AmqpError;
use crate::exchange::RabbitExchange;
use crate::subscriber::LapinSubscriber;

/// How long a partial batch waits for more deliveries before it goes to the handler, unless
/// [`RabbitQueue::batch_wait`] says otherwise.
///
/// Fifty milliseconds is a compromise for a network transport: long enough for the broker to
/// push the rest of a prefetch window across the connection (many round trips on any healthy
/// link), short enough to bound how long the tail of a backlog sits unhandled.
const DEFAULT_BATCH_WAIT: Duration = Duration::from_millis(50);

/// The queue implementation selected at declaration time.
///
/// Only used when the broker declares topology; an existing queue keeps whatever type it was
/// created with (`x-queue-type` cannot change after creation).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum QueueType {
    /// The classic single-node queue implementation.
    Classic,
    /// The Raft-replicated quorum queue implementation; requires a durable queue.
    Quorum,
}

impl QueueType {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Classic => "classic",
            Self::Quorum => "quorum",
        }
    }
}

/// Describes one queue subscription: the queue, its expected settings, and its bindings.
///
/// Descriptors describe the EXPECTED topology for routing; by default nothing is created on the
/// broker (managing infrastructure is the user's job). Opt in to declaration per broker with
/// [`declare_topology(true)`](crate::LapinBroker::declare_topology).
///
/// # Examples
///
/// ```
/// use ruststream_lapin::{QueueType, RabbitExchange, RabbitQueue};
///
/// let orders = RabbitQueue::new("orders")
///     .queue_type(QueueType::Quorum)
///     .bind(RabbitExchange::topic("events"), "order.*")
///     .dead_letter_exchange("dead-letters");
/// assert_eq!(orders.name(), "orders");
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct RabbitQueue {
    name: String,
    durable: bool,
    exclusive: bool,
    auto_delete: bool,
    queue_type: Option<QueueType>,
    bindings: Vec<(RabbitExchange, String)>,
    arguments: FieldTable,
    prefetch: Option<NonZeroU16>,
    batch_wait: Duration,
    delay: Option<Delay>,
}

impl RabbitQueue {
    /// Describes the queue `name` with the defaults: durable, shared, not auto-deleted.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            durable: true,
            exclusive: false,
            auto_delete: false,
            queue_type: None,
            bindings: Vec::new(),
            arguments: FieldTable::default(),
            prefetch: None,
            batch_wait: DEFAULT_BATCH_WAIT,
            delay: None,
        }
    }

    /// Whether the queue survives a broker restart. Defaults to `true`.
    #[must_use]
    pub fn durable(mut self, durable: bool) -> Self {
        self.durable = durable;
        self
    }

    /// Whether the queue is exclusive to this connection. Defaults to `false`.
    #[must_use]
    pub fn exclusive(mut self, exclusive: bool) -> Self {
        self.exclusive = exclusive;
        self
    }

    /// Whether the queue is deleted when its last consumer disconnects. Defaults to `false`.
    #[must_use]
    pub fn auto_delete(mut self, auto_delete: bool) -> Self {
        self.auto_delete = auto_delete;
        self
    }

    /// The queue type to declare, overriding the broker-wide
    /// [`default_queue_type`](crate::LapinBroker::default_queue_type).
    ///
    /// When neither is set no `x-queue-type` argument is sent and the server default applies.
    #[must_use]
    pub fn queue_type(mut self, queue_type: QueueType) -> Self {
        self.queue_type = Some(queue_type);
        self
    }

    /// Binds the queue to `exchange` under `routing_key`.
    ///
    /// Call repeatedly for multiple bindings. Without any binding the queue only receives
    /// messages published to the default exchange under the queue name.
    #[must_use]
    pub fn bind(mut self, exchange: RabbitExchange, routing_key: impl Into<String>) -> Self {
        self.bindings.push((exchange, routing_key.into()));
        self
    }

    /// Dead-letters rejected messages to `exchange` (the `x-dead-letter-exchange` argument).
    ///
    /// A handler returning drop settles with `basic.reject(requeue = false)`, which routes the
    /// message there.
    #[must_use]
    pub fn dead_letter_exchange(mut self, exchange: impl Into<String>) -> Self {
        self.arguments.insert(
            ShortString::from("x-dead-letter-exchange"),
            AMQPValue::LongString(exchange.into().into()),
        );
        self
    }

    /// Overrides the routing key dead-lettered messages carry (`x-dead-letter-routing-key`).
    #[must_use]
    pub fn dead_letter_routing_key(mut self, routing_key: impl Into<String>) -> Self {
        self.arguments.insert(
            ShortString::from("x-dead-letter-routing-key"),
            AMQPValue::LongString(routing_key.into().into()),
        );
        self
    }

    /// Sets one raw declaration argument (`x-...`), passed through verbatim.
    ///
    /// # Panics
    ///
    /// Panics if `name` exceeds 255 bytes (the AMQP short-string limit); argument names are
    /// compile-time constants in practice.
    #[must_use]
    pub fn argument(mut self, name: impl Into<String>, value: AMQPValue) -> Self {
        self.arguments.insert(ShortString::from(name.into()), value);
        self
    }

    /// Replaces the whole raw declaration argument table (`x-...` passthrough).
    #[must_use]
    pub fn arguments(mut self, arguments: FieldTable) -> Self {
        self.arguments = arguments;
        self
    }

    /// Caps unacknowledged deliveries in flight for this subscription (`basic.qos`),
    /// overriding the broker-wide [`prefetch`](crate::LapinBroker::prefetch).
    ///
    /// This is the back-pressure window for the subscriber stream. When neither is set the
    /// server imposes no prefetch limit.
    ///
    /// The count is a [`NonZeroU16`] because AMQP reads `basic.qos(0)` as "no limit", the exact
    /// opposite of a cap: the zero sentinel is unrepresentable here, and leaving the prefetch
    /// unset is how "unlimited" is spelled.
    #[must_use]
    pub fn prefetch(mut self, prefetch: NonZeroU16) -> Self {
        self.prefetch = Some(prefetch);
        self
    }

    /// Caps how long a partial batch waits for more deliveries before it goes to the handler.
    /// Defaults to 50 ms.
    ///
    /// AMQP delivers one message at a time, so a batch handler's `batch(n)` is honoured by
    /// collecting deliveries on the client; how big a batch may be is the registration's word, and
    /// this is the other half of the trade-off - the ceiling on how long a batch that never fills
    /// keeps its deliveries. Raise it on a slow link or a sparse queue where fuller batches are
    /// worth the wait; lower it where a batch arriving late costs more than a batch arriving
    /// short. It has no effect on a subscription without a batch handler.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    ///
    /// use ruststream_lapin::RabbitQueue;
    ///
    /// let orders = RabbitQueue::new("orders").batch_wait(Duration::from_millis(200));
    /// # let _ = orders;
    /// ```
    #[must_use]
    pub fn batch_wait(mut self, batch_wait: Duration) -> Self {
        self.batch_wait = batch_wait;
        self
    }

    /// Makes `retry_after` / `nack_after` native, routing delayed redeliveries through a broker
    /// waiting queue instead of the core in-process fallback.
    ///
    /// Without this the runtime handles a delay with its broker-agnostic deferred re-publish
    /// (at-most-once over the delay window, held in the service). With it, a delayed message is
    /// re-published to the [`Delay`] waiting queue with a per-message TTL and dead-lettered back
    /// to this queue when it fires, so the delayed copy lives on the broker.
    ///
    /// The waiting queue is infrastructure: it is declared only when the broker opts into
    /// [`declare_topology`](crate::LapinBroker::declare_topology); otherwise provision it yourself.
    #[must_use]
    pub fn delay(mut self, delay: Delay) -> Self {
        self.delay = Some(delay);
        self
    }

    /// The queue name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn is_durable(&self) -> bool {
        self.durable
    }

    pub(crate) fn is_exclusive(&self) -> bool {
        self.exclusive
    }

    pub(crate) fn is_auto_delete(&self) -> bool {
        self.auto_delete
    }

    pub(crate) fn queue_type_or(&self, broker_default: Option<QueueType>) -> Option<QueueType> {
        self.queue_type.or(broker_default)
    }

    pub(crate) fn bindings(&self) -> &[(RabbitExchange, String)] {
        &self.bindings
    }

    pub(crate) fn declare_arguments(&self) -> &FieldTable {
        &self.arguments
    }

    pub(crate) fn prefetch_or(&self, broker_default: Option<NonZeroU16>) -> Option<NonZeroU16> {
        self.prefetch.or(broker_default)
    }

    pub(crate) const fn batch_wait_of(&self) -> Duration {
        self.batch_wait
    }

    pub(crate) fn delay_config(&self) -> Option<&Delay> {
        self.delay.as_ref()
    }
}

/// The descriptor is its own source, so the manual path's `subscriber(source, body)` takes a
/// `RabbitQueue` where the attribute path writes it in the decorator.
impl IntoSource for RabbitQueue {
    type Source = Self;

    fn into_source(self) -> Self {
        self
    }
}

impl SubscriptionSource<ConnectedLapinBroker> for RabbitQueue {
    type Subscriber = LapinSubscriber;

    fn name(&self) -> &str {
        &self.name
    }

    async fn subscribe(
        self,
        connected: &ConnectedLapinBroker,
    ) -> Result<Self::Subscriber, AmqpError> {
        connected.subscribe(self).await
    }

    /// The queue name: on the default exchange a routing key addresses the queue that carries it,
    /// so that is where the runtime's deferred `retry_after` copy reaches this subscription again.
    ///
    /// The answer holds for a retry publisher on the default exchange, which is what
    /// [`LapinPublish`](crate::LapinPublish) is unless configured otherwise. A publisher aimed at
    /// a topic or direct exchange reaches the queue only through a binding under this name, so
    /// bind it there or leave the retry publisher on the default exchange.
    fn redelivery_address(
        &self,
        _connected: &ConnectedLapinBroker,
    ) -> impl Future<Output = Result<Option<RedeliveryAddress>, AmqpError>> {
        // The name is on the descriptor, so nothing has to be asked of the broker.
        ready(Ok(Some(RedeliveryAddress::new(self.name.clone()))))
    }
}

#[cfg(feature = "testing")]
impl SubscriptionSource<crate::testing::ConnectedLapinTestBroker> for RabbitQueue {
    type Subscriber = crate::testing::LapinTestSubscriber;

    fn name(&self) -> &str {
        &self.name
    }

    async fn subscribe(
        self,
        connected: &crate::testing::ConnectedLapinTestBroker,
    ) -> Result<Self::Subscriber, AmqpError> {
        connected.subscribe(self.name).await
    }

    /// The queue name, the same answer the live broker gives: the in-process transport routes by
    /// exact queue name on the default-exchange model, so a retry publisher reaches the
    /// subscription in a test exactly where it reaches it on a server.
    fn redelivery_address(
        &self,
        _connected: &crate::testing::ConnectedLapinTestBroker,
    ) -> impl Future<Output = Result<Option<RedeliveryAddress>, AmqpError>> {
        ready(Ok(Some(RedeliveryAddress::new(self.name.clone()))))
    }
}
