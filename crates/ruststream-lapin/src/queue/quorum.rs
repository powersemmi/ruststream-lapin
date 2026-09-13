//! The quorum queue descriptor.

use std::num::NonZeroU16;
use std::time::Duration;

use lapin::types::{AMQPValue, FieldTable};
#[cfg(feature = "asyncapi")]
use ruststream::asyncapi::Bindings;
use ruststream::runtime::IntoSource;
use ruststream::{BrokerMoves, RetryDeclaration, SubscriptionSource};

use super::spec::{
    DEAD_LETTER_EXCHANGE, DEAD_LETTER_ROUTING_KEY, DELIVERY_LIMIT, QueueKind, QueueSpec,
    queue_settings,
};
use crate::broker::ConnectedLapinBroker;
use crate::delay::Delay;
use crate::error::AmqpError;
use crate::exchange::RabbitExchange;
use crate::subscriber::LapinSubscriber;

/// Describes one quorum-queue subscription: the queue, its expected settings, and its bindings.
///
/// A quorum queue counts the deliveries a message has spent and ends a poison loop itself: with
/// an `x-delivery-limit` and a dead-letter route it carries a message away once its deliveries
/// run out, whether or not a service is running to count them. So the retries of a registration
/// mounted here are the broker's, and `out_retry(policy)` does not compile on it - there is no
/// copy of this process's to publish. A registration declares the pair at the mount site:
///
/// ```text
/// b.include(charge).max_attempts(nonzero!(3)).dead_letter("orders.dead");
/// ```
///
/// The pair becomes this queue's `x-delivery-limit` and dead-letter route when the broker opts
/// into [`declare_topology(true)`](crate::LapinBroker::declare_topology). A queue declared
/// elsewhere carries its own arguments, so subscribing then refuses the declaration rather than
/// promising a cap nothing applies; configure the queue on the broker and mount it without the
/// pair. The crate reads the queue's count either way.
///
/// A quorum queue is durable, shared and permanent by definition, so those three settings are not
/// here to be set wrong. For the single-node implementation take [`RabbitQueue`](crate::RabbitQueue).
///
/// # Examples
///
/// ```
/// use ruststream_lapin::{RabbitExchange, RabbitQuorumQueue};
///
/// let orders = RabbitQuorumQueue::new("orders")
///     .bind(RabbitExchange::topic("events"), "order.*")
///     .dead_letter_exchange("dead-letters")
///     .delivery_limit(4);
/// assert_eq!(orders.name(), "orders");
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct RabbitQuorumQueue {
    spec: QueueSpec,
    retry: RetryDeclaration,
}

impl RabbitQuorumQueue {
    /// Describes the quorum queue `name`.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            spec: QueueSpec::new(QueueKind::Quorum, name.into()),
            retry: RetryDeclaration::new(),
        }
    }

    /// How often the queue returns a message before it carries it away (`x-delivery-limit`).
    ///
    /// The argument counts the returns a message survives, so it is one less than the number of
    /// deliveries the queue allows: `delivery_limit(2)` hands a message out three times. Where it
    /// goes then is [`dead_letter_exchange`](Self::dead_letter_exchange); a limit with no
    /// dead-letter route drops the spent message instead.
    ///
    /// This is the queue's own policy, for a queue whose retries are not one registration's
    /// business. A mount site that declares `max_attempts(n).dead_letter(..)` is the more specific
    /// statement and is written over this.
    #[must_use]
    pub fn delivery_limit(mut self, returns: u32) -> Self {
        self.spec
            .set(DELIVERY_LIMIT, AMQPValue::LongLongInt(i64::from(returns)));
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
    /// A native dead-letter policy needs both halves, and both halves need a queue this service
    /// declares - a queue declared elsewhere carries the arguments whoever declared it gave it.
    /// Neither is knowable before the broker is configured, which is why this is a startup error
    /// and not a compile error.
    pub(crate) fn check_retry(&self, declares_topology: bool) -> Result<(), AmqpError> {
        match (self.retry.max_attempts(), self.retry.dead_letter()) {
            // Both halves, on a queue this service declares: the declaration becomes arguments.
            (Some(_), Some(_)) if declares_topology => Ok(()),
            // Nothing declared: the queue keeps whatever policy it was given.
            (None, None) => Ok(()),
            (Some(_), None) => Err(AmqpError::InvalidOptions(format!(
                "quorum queue {:?} was mounted with `max_attempts(..)` and no `dead_letter(..)`: \
                 the queue carries a spent delivery away only when it knows where to, and a \
                 delivery limit on its own drops it. Declare both at the mount site, or neither \
                 and let the queue keep its own policy",
                self.name(),
            ))),
            (None, Some(_)) => Err(AmqpError::InvalidOptions(format!(
                "quorum queue {:?} was mounted with `dead_letter(..)` and no `max_attempts(..)`: \
                 nothing tells the queue when a delivery is spent, so the destination is never \
                 reached. Declare both at the mount site, or neither and let the queue keep its \
                 own policy",
                self.name(),
            ))),
            (Some(_), Some(_)) => Err(AmqpError::InvalidOptions(format!(
                "quorum queue {:?} applies `max_attempts(..).dead_letter(..)` as its own \
                 arguments, and this service does not declare it. Enable \
                 `declare_topology(true)`, or configure `x-delivery-limit` and \
                 `x-dead-letter-exchange` on the queue itself and mount the handler without the \
                 pair",
                self.name(),
            ))),
        }
    }
}

queue_settings!(RabbitQuorumQueue);

/// The descriptor is its own source, so the manual path's `subscriber(source, body)` takes a
/// `RabbitQuorumQueue` where the attribute path writes it in the decorator.
impl IntoSource for RabbitQuorumQueue {
    type Source = Self;

    fn into_source(self) -> Self {
        self
    }
}

impl SubscriptionSource<ConnectedLapinBroker> for RabbitQuorumQueue {
    type Subscriber = LapinSubscriber;

    /// The queue moves a spent delivery itself: its `x-delivery-limit` counts the deliveries and
    /// its dead-letter route takes the message that runs out of them, with nothing published by
    /// this process. That is what makes `out_retry(policy)` a compile error here, and what keeps
    /// the cap holding while the service is down.
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

    /// Takes the registration's cap and dead-letter destination into the descriptor, so the queue
    /// this broker declares carries them as topology.
    fn declare_retry(mut self, declaration: &RetryDeclaration) -> Self {
        self.retry = declaration.clone();
        self
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

#[cfg(feature = "testing")]
impl SubscriptionSource<crate::testing::ConnectedLapinTestBroker> for RabbitQuorumQueue {
    type Subscriber = crate::testing::LapinTestSubscriber;

    /// The copy path the live broker takes, so a registration that compiles against one compiles
    /// against the other: the transport's stand-in counts the deliveries and carries a spent one
    /// away the way the queue does.
    type Copies = BrokerMoves;

    fn name(&self) -> &str {
        &self.spec.name
    }

    async fn subscribe(
        self,
        connected: &crate::testing::ConnectedLapinTestBroker,
    ) -> Result<Self::Subscriber, AmqpError> {
        connected.subscribe_to(&self).await
    }

    /// Recorded as the live descriptor records it, and applied the same way: the in-process stand
    /// declares the queue it opens, so the arguments the declaration produces are what the
    /// subscription behaves by.
    fn declare_retry(mut self, declaration: &RetryDeclaration) -> Self {
        self.retry = declaration.clone();
        self
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
