//! The classic queue descriptor.

use std::num::NonZeroU16;
use std::time::Duration;

use lapin::types::{AMQPValue, FieldTable};
#[cfg(feature = "asyncapi")]
use ruststream::asyncapi::Bindings;
use ruststream::runtime::IntoSource;
use ruststream::{BrokerMoves, RetryDeclaration, SubscriptionSource};

use super::spec::{
    Binding, DEAD_LETTER_EXCHANGE, DEAD_LETTER_ROUTING_KEY, QueueKind, QueueSpec, queue_settings,
};
#[cfg(feature = "asyncapi")]
use crate::bindings::{consumer_operation, queue_channel};
use crate::broker::ConnectedLapinBroker;
use crate::delay::Delay;
use crate::error::AmqpError;
use crate::exchange::RabbitExchange;
use crate::subscriber::LapinSubscriber;

/// Describes one classic-queue subscription: the queue, its expected settings, and its bindings.
///
/// A classic queue moves a rejected delivery to its dead-letter route itself, and keeps no count
/// of the deliveries a message has spent. So a registration mounted here takes
/// `dead_letter(..)`, which becomes the queue's `x-dead-letter-exchange` and
/// `x-dead-letter-routing-key`: a delivery the handler drops leaves for that queue, moved by the
/// server, and the service publishes nothing. `max_attempts(..)` has nothing on a classic queue to
/// apply it, so subscribing refuses it; the queue that counts deliveries and ends a poison loop is
/// a [`RabbitQuorumQueue`](crate::RabbitQuorumQueue). `out_retry(policy)` does not compile here:
/// no copy of a retry is published by this process.
///
/// Descriptors describe the EXPECTED topology for routing; by default nothing is created on the
/// broker (managing infrastructure is the user's job). Opt in to declaration per broker with
/// [`declare_topology(true)`](crate::LapinBroker::declare_topology). A declared queue carries the
/// dead-letter arguments the mount site asked for, and the server refuses to declare a queue that
/// already exists with other ones, which stops the subscription at startup with an error naming
/// the queue and the arguments. A queue this service does not declare keeps the arguments the
/// operator gave it, and `dead_letter(..)` then documents that route rather than setting it.
///
/// # Examples
///
/// ```
/// use ruststream_lapin::prelude::*;
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct OrderPlaced {
///     id: u64,
/// }
///
/// #[subscriber(RabbitQueue::new("orders")
///     .bind(RabbitExchange::topic("events"), "order.*")
///     .dead_letter_exchange("dead-letters"))]
/// async fn on_order(event: &OrderPlaced) -> HandlerOutcome {
///     if event.id == 0 {
///         // Rejected without requeue: the queue's dead-letter exchange takes it.
///         return HandlerOutcome::drop();
///     }
///     HandlerOutcome::ack()
/// }
///
/// fn app() -> RustStream {
///     let broker = LapinBroker::new("amqp://localhost:5672").declare_topology(true);
///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
///         b.include(on_order);
///     })
/// }
/// ```
///
/// A slice parameter asks for a batch, and the mount site says how big it may get:
///
/// ```
/// use std::time::Duration;
///
/// use ruststream_lapin::prelude::*;
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Settlement {
///     id: u64,
/// }
///
/// #[subscriber(RabbitQueue::new("settlements")
///     .prefetch(nonzero!(64))
///     .batch_wait(Duration::from_millis(200)))]
/// async fn on_settlements(settlements: &[Settlement]) -> HandlerOutcome {
///     tracing::info!(count = settlements.len(), "settling");
///     HandlerOutcome::ack()
/// }
///
/// fn app() -> RustStream {
///     let broker = LapinBroker::new("amqp://localhost:5672").declare_topology(true);
///     RustStream::new(AppInfo::new("settlements", "0.1.0")).with_broker(broker, |b| {
///         b.include(on_settlements.batch(nonzero!(32)));
///     })
/// }
/// ```
///
/// A delivery the handler drops leaves for the declared dead-letter queue, moved by the server:
///
/// ```
/// use ruststream_lapin::prelude::*;
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Refund {
///     id: u64,
/// }
///
/// #[subscriber(RabbitQueue::new("refunds"))]
/// async fn on_refund(refund: &Refund) -> HandlerOutcome {
///     tracing::warn!(refund.id, "the refund names no order, setting it aside");
///     HandlerOutcome::drop()
/// }
///
/// fn app() -> RustStream {
///     let broker = LapinBroker::new("amqp://localhost:5672").declare_topology(true);
///     RustStream::new(AppInfo::new("refunds", "0.1.0")).with_broker(broker, |b| {
///         b.include(on_refund).dead_letter("refunds.dead");
///     })
/// }
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct RabbitQueue {
    spec: QueueSpec,
    retry: RetryDeclaration,
}

impl RabbitQueue {
    /// Describes the classic queue `name` with the defaults: durable, shared, not auto-deleted.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            spec: QueueSpec::new(QueueKind::Classic, name.into()),
            retry: RetryDeclaration::new(),
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

    /// What the mount site declared about this registration's retries, applied when the queue is
    /// declared.
    pub(crate) const fn retry(&self) -> &RetryDeclaration {
        &self.retry
    }

    /// Refuses a declaration the queue cannot carry, before the subscription opens.
    ///
    /// A classic queue keeps no delivery count, so a cap has nothing on the server to apply it,
    /// and nothing in this process counts for it either: the server moves a rejected delivery
    /// itself.
    // Why a startup error and not a compile error: the mount site's `max_attempts(..)` step is
    // the framework's, and its bounds read nothing off the descriptor it is mounted on.
    pub(crate) fn check_retry(&self) -> Result<(), AmqpError> {
        if self.retry.max_attempts().is_some() {
            return Err(AmqpError::InvalidOptions(format!(
                "classic queue {:?} was mounted with `max_attempts(..)`: a classic queue keeps no \
                 delivery count, so nothing applies the cap. Mount the handler on a \
                 `RabbitQuorumQueue`, whose `x-delivery-limit` applies it, or drop \
                 `max_attempts(..)` and keep `dead_letter(..)`",
                self.name(),
            )));
        }
        Ok(())
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

    /// The server moves a rejected delivery to the queue's dead-letter route itself, with nothing
    /// published by this process. That is what makes `out_retry(policy)` a compile error here, and
    /// what keeps a dead-lettered message from reaching its destination twice.
    type Copies = BrokerMoves;

    fn name(&self) -> &str {
        &self.spec.name
    }

    async fn subscribe(
        self,
        connected: &ConnectedLapinBroker,
    ) -> Result<Self::Subscriber, AmqpError> {
        connected.subscribe(self).await
    }

    /// Takes the registration's dead-letter destination into the descriptor, so the queue this
    /// broker declares carries it as topology.
    fn declare_retry(mut self, declaration: &RetryDeclaration) -> Self {
        self.retry = declaration.clone();
        self
    }

    /// The queue this subscription consumes, as the `amqp` binding names its settings.
    #[cfg(feature = "asyncapi")]
    fn channel_bindings(&self) -> Bindings {
        queue_channel(&self.spec)
    }

    /// The consumer's own half: this crate acknowledges by hand on every subscription.
    #[cfg(feature = "asyncapi")]
    fn operation_bindings(&self) -> Bindings {
        consumer_operation()
    }
}
