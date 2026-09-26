//! The classic queue descriptor.

use std::future::{Future, ready};
use std::num::NonZeroU16;
use std::time::Duration;

use lapin::types::{AMQPValue, FieldTable};
#[cfg(feature = "asyncapi")]
use ruststream::asyncapi::Bindings;
use ruststream::runtime::IntoSource;
use ruststream::{AddressedCopies, RedeliveryAddress, RedeliveryAddressed, SubscriptionSource};

use super::spec::{
    Binding, DEAD_LETTER_EXCHANGE, DEAD_LETTER_ROUTING_KEY, QueueKind, QueueSpec, queue_settings,
};
use crate::broker::ConnectedLapinBroker;
use crate::delay::Delay;
use crate::error::AmqpError;
use crate::exchange::RabbitExchange;
use crate::subscriber::LapinSubscriber;

/// Describes one classic-queue subscription: the queue, its expected settings, and its bindings.
///
/// The classic queue is the single-node implementation, and it counts nothing: a delivery says
/// only that it has been seen before, never how often. So a registration's `max_attempts(n)` is
/// the runtime's to apply, counted with the framework's own header on the copies it publishes,
/// and `out_retry(policy)` names the publisher those copies leave through. A queue that should
/// end a poison loop on the server instead is a [`RabbitQuorumQueue`](crate::RabbitQuorumQueue).
///
/// Descriptors describe the EXPECTED topology for routing; by default nothing is created on the
/// broker (managing infrastructure is the user's job). Opt in to declaration per broker with
/// [`declare_topology(true)`](crate::LapinBroker::declare_topology).
///
/// # Examples
///
/// ```
/// use ruststream_lapin::{RabbitExchange, RabbitQueue};
///
/// let orders = RabbitQueue::new("orders")
///     .bind(RabbitExchange::topic("events"), "order.*")
///     .dead_letter_exchange("dead-letters");
/// assert_eq!(orders.name(), "orders");
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct RabbitQueue {
    spec: QueueSpec,
}

impl RabbitQueue {
    /// Describes the classic queue `name` with the defaults: durable, shared, not auto-deleted.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            spec: QueueSpec::new(QueueKind::Classic, name.into()),
        }
    }

    /// Whether the queue survives a broker restart. Defaults to `true`.
    ///
    /// `RabbitMQ` 4 denies transient non-exclusive queues, so a queue declared `durable(false)`
    /// has to be [`exclusive`](Self::exclusive) too.
    #[must_use]
    pub fn durable(mut self, durable: bool) -> Self {
        self.spec.durable = durable;
        self
    }

    /// Whether the queue is exclusive to this connection. Defaults to `false`.
    #[must_use]
    pub fn exclusive(mut self, exclusive: bool) -> Self {
        self.spec.exclusive = exclusive;
        self
    }

    /// Whether the queue is deleted when its last consumer disconnects. Defaults to `false`.
    #[must_use]
    pub fn auto_delete(mut self, auto_delete: bool) -> Self {
        self.spec.auto_delete = auto_delete;
        self
    }

    pub(crate) const fn spec(&self) -> &QueueSpec {
        &self.spec
    }
}

queue_settings!(RabbitQueue);

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

    /// The service publishes the copies of a retry, and the queue name is where they go: on the
    /// default exchange a routing key addresses the queue that carries it.
    ///
    /// A classic queue keeps no counter and has no delivery limit, so there is nothing on the
    /// server to end a retry loop; the runtime counts and the mount site names the publisher the
    /// copies leave through with `out_retry(policy)`.
    type Copies = AddressedCopies;

    fn name(&self) -> &str {
        &self.spec.name
    }

    async fn subscribe(
        self,
        connected: &ConnectedLapinBroker,
    ) -> Result<Self::Subscriber, AmqpError> {
        connected.subscribe(self).await
    }

    /// The queue this subscription consumes, as the `amqp` binding names its settings.
    #[cfg(feature = "asyncapi")]
    fn channel_bindings(&self) -> Bindings {
        crate::bindings::queue_channel(&self.spec)
    }

    /// The consumer's own half: this crate acknowledges by hand on every subscription.
    #[cfg(feature = "asyncapi")]
    fn operation_bindings(&self) -> Bindings {
        crate::bindings::consumer_operation()
    }
}

/// The queue name: on the default exchange a routing key addresses the queue that carries it, so
/// that is where the runtime's retry copy reaches this subscription again.
///
/// The answer holds for a retry publisher on the default exchange, which is what
/// [`LapinPublish`](crate::LapinPublish) is unless configured otherwise. A publisher aimed at a
/// topic or direct exchange reaches the queue only through a binding under this name, so bind it
/// there or leave the retry publisher on the default exchange.
impl RedeliveryAddressed<ConnectedLapinBroker> for RabbitQueue {
    fn redelivery_address(
        &self,
        _connected: &ConnectedLapinBroker,
    ) -> impl Future<Output = Result<RedeliveryAddress, AmqpError>> + Send {
        // The name is on the descriptor, so nothing has to be asked of the broker.
        ready(Ok(RedeliveryAddress::new(self.spec.name.clone())))
    }
}
