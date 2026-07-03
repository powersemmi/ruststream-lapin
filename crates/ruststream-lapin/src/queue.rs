//! The queue descriptor: what a subscription binds to and, optionally, expects to exist.

use lapin::types::{AMQPValue, FieldTable, ShortString};
use ruststream::SubscriptionSource;

use crate::broker::LapinBroker;
use crate::error::AmqpError;
use crate::exchange::RabbitExchange;
use crate::subscriber::LapinSubscriber;

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
/// [`declare_topology(true)`](LapinBroker::declare_topology).
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
    prefetch: Option<u16>,
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
    /// [`default_queue_type`](LapinBroker::default_queue_type).
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
    /// overriding the broker-wide [`prefetch`](LapinBroker::prefetch).
    ///
    /// This is the back-pressure window for the subscriber stream. When neither is set the
    /// server imposes no prefetch limit.
    #[must_use]
    pub fn prefetch(mut self, prefetch: u16) -> Self {
        self.prefetch = Some(prefetch);
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

    pub(crate) fn prefetch_or(&self, broker_default: Option<u16>) -> Option<u16> {
        self.prefetch.or(broker_default)
    }
}

impl SubscriptionSource<LapinBroker> for RabbitQueue {
    type Subscriber = LapinSubscriber;

    fn name(&self) -> &str {
        &self.name
    }

    async fn subscribe(self, broker: &LapinBroker) -> Result<Self::Subscriber, AmqpError> {
        broker.subscribe(self).await
    }
}

#[cfg(feature = "testing")]
impl SubscriptionSource<crate::testing::LapinTestBroker> for RabbitQueue {
    type Subscriber = crate::testing::LapinTestSubscriber;

    fn name(&self) -> &str {
        &self.name
    }

    async fn subscribe(
        self,
        broker: &crate::testing::LapinTestBroker,
    ) -> Result<Self::Subscriber, AmqpError> {
        broker.subscribe(self.name).await
    }
}
