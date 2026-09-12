//! The declaration half of publishing: the policies and what they pair into.
//!
//! A policy holds nothing but publish options, so it is constructible anywhere - in a router
//! definition, in configuration, before anything connects. Pairing it with a
//! [`ConnectedLapinBroker`] produces the live publisher (see [`crate::publisher`]), which is the
//! only value with a publish surface. The publishing mode is a policy transition:
//! [`LapinPublish::confirms`] and [`LapinPublish::server_tx`] move to the transactional policies,
//! keeping the options.

use std::future::{Future, ready};
use std::time::Duration;

use ruststream::{PairError, PublishPolicy};

use crate::broker::ConnectedLapinBroker;
use crate::publish_step::LapinPublishOptions;
use crate::publisher::{ConfirmsPublisher, LapinPublisher, ServerTxPublisher};

use self::sealed::Sealed;

pub(crate) mod sealed {
    /// Seals [`LapinPublishPolicy`](super::LapinPublishPolicy) and its in-process counterpart
    /// `LapinTestPublishPolicy`: pairing an AMQP publisher opens no channel of its own, and the
    /// synchronous [`publisher`](crate::ConnectedLapinBroker::publisher) accessor depends on
    /// that.
    pub trait Sealed {}

    impl Sealed for super::LapinPublish {}
    impl Sealed for super::ConfirmsPublish {}
    impl Sealed for super::ServerTxPublish {}
    impl Sealed for crate::requester::LapinRequest {}
}

/// What every `RabbitMQ` publish policy carries: where to publish, and the per-message properties
/// a publish through it takes unless its own call site says otherwise.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PublishOptions {
    pub(crate) exchange: String,
    pub(crate) defaults: LapinPublishOptions,
}

impl PublishOptions {
    /// The settings one publish carries: what its call site adjusted, over the policy's defaults.
    pub(crate) fn resolve(&self, call: Option<&LapinPublishOptions>) -> LapinPublishOptions {
        call.map_or(self.defaults, |call| call.over(&self.defaults))
    }
}

impl Default for PublishOptions {
    fn default() -> Self {
        Self {
            exchange: String::new(),
            defaults: LapinPublishOptions {
                persistent: Some(true),
                ..LapinPublishOptions::default()
            },
        }
    }
}

/// Writes the four policy setters every publish policy of this crate shares.
///
/// They differ only in the type they return, and a policy is a newtype over [`PublishOptions`],
/// so the bodies would be four copies each. The documentation is written once here and reaches
/// every policy's rustdoc.
macro_rules! publish_policy_settings {
    ($policy:ident) => {
        impl $policy {
            /// What this policy hands the publisher it pairs into.
            ///
            /// The live publishers take the value by move at `bind`; this borrow is for the
            /// in-process stand-ins, which clone it.
            #[cfg(feature = "testing")]
            pub(crate) const fn publish_options(&self) -> &PublishOptions {
                &self.0
            }

            /// Publishes to `exchange` instead of the default exchange.
            pub fn exchange(mut self, exchange: impl Into<String>) -> Self {
                self.0.exchange = exchange.into();
                self
            }

            /// Whether messages are marked persistent (delivery mode 2). Defaults to `true`.
            ///
            /// One message departs from this with the publish builder's
            /// [`persistent`](crate::LapinPublishSteps::persistent) step.
            pub fn persistent(mut self, persistent: bool) -> Self {
                self.0.defaults.persistent = Some(persistent);
                self
            }

            /// The AMQP `priority` property messages carry. Unset by default, so the broker sees
            /// no priority at all.
            ///
            /// It only orders deliveries on a queue declared with `x-max-priority`. One message
            /// departs from this with the publish builder's
            /// [`priority`](crate::LapinPublishSteps::priority) step.
            pub fn priority(mut self, priority: u8) -> Self {
                self.0.defaults.priority = Some(priority);
                self
            }

            /// The AMQP per-message `expiration` (TTL) messages carry: the broker drops one once
            /// `ttl` has passed without it being consumed. Unset by default, so messages do not
            /// expire.
            ///
            /// One message departs from this with the publish builder's
            /// [`expiration`](crate::LapinPublishSteps::expiration) step.
            pub fn expiration(mut self, ttl: Duration) -> Self {
                self.0.defaults.expiration = Some(ttl);
                self
            }
        }
    };
}

/// A publish policy that pairs with a connected `RabbitMQ` broker without opening a channel.
///
/// All of this crate's policies hold nothing but publish options, so bringing one alive is a
/// constructor call rather than broker work. That is what lets
/// [`ConnectedLapinBroker::publisher`] be synchronous; [`PublishPolicy::pair`], the
/// framework-side entry point, delegates here.
pub trait LapinPublishPolicy: PublishPolicy<ConnectedLapinBroker> + Sealed {
    /// Pairs the policy with the connected broker, producing the live publisher.
    #[must_use]
    fn bind(self, connected: &ConnectedLapinBroker) -> Self::Live;
}

/// The fire-and-forget publish policy: pure declaration, constructible anywhere.
///
/// [`OutgoingMessage::name`](ruststream::OutgoingMessage::name) is the routing key; the target
/// exchange is a property of the policy (the default exchange unless
/// [`exchange`](Self::exchange) says otherwise). On the default exchange the routing key
/// addresses the queue with that name.
///
/// The policy is also where the per-message AMQP properties get their defaults:
/// [`persistent`](Self::persistent), [`priority`](Self::priority) and
/// [`expiration`](Self::expiration) fix what every publish through it carries, and the publish
/// builder's own steps ([`LapinPublishSteps`](crate::LapinPublishSteps)) adjust one message over
/// them.
///
/// It pairs into [`LapinPublisher`], and it is the broker's
/// [`DefaultPublish`](ruststream::DefaultPublish) policy, so a `publish("dest")` handler mounted
/// without an explicit publisher replies through it. [`confirms`](Self::confirms) and
/// [`server_tx`](Self::server_tx) move to the transactional policies, keeping the settings.
///
/// # Examples
///
/// ```
/// use ruststream_lapin::LapinPublish;
///
/// let events = LapinPublish::default().exchange("events").priority(3);
/// let shipments = LapinPublish::default().confirms();
/// # let _ = (events, shipments);
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[must_use]
pub struct LapinPublish(PublishOptions);

impl LapinPublish {
    /// Moves to the policy that awaits broker confirms, with buffering transactions.
    ///
    /// The recommended transactional publisher: durable and much faster than AMQP server
    /// transactions.
    pub fn confirms(self) -> ConfirmsPublish {
        ConfirmsPublish(self.0)
    }

    /// Moves to the policy backed by AMQP server transactions (`tx.select`).
    ///
    /// Server-side atomicity, at the cost of a synchronous commit round trip that is
    /// significantly slower than [`confirms`](Self::confirms).
    pub fn server_tx(self) -> ServerTxPublish {
        ServerTxPublish(self.0)
    }
}

publish_policy_settings!(LapinPublish);

impl PublishPolicy<ConnectedLapinBroker> for LapinPublish {
    type Live = LapinPublisher;

    fn pair(
        self,
        connected: &ConnectedLapinBroker,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(self.bind(connected)))
    }
}

impl LapinPublishPolicy for LapinPublish {
    fn bind(self, connected: &ConnectedLapinBroker) -> Self::Live {
        LapinPublisher::new(connected, self.0)
    }
}

/// The confirm-transactional publish policy: same options as [`LapinPublish`], pairing into
/// [`ConfirmsPublisher`].
///
/// Reached with [`LapinPublish::confirms`].
///
/// # Examples
///
/// ```
/// use ruststream_lapin::LapinPublish;
///
/// let shipments = LapinPublish::default().exchange("shipments").confirms();
/// # let _ = shipments;
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[must_use]
pub struct ConfirmsPublish(PublishOptions);

publish_policy_settings!(ConfirmsPublish);

impl PublishPolicy<ConnectedLapinBroker> for ConfirmsPublish {
    type Live = ConfirmsPublisher;

    fn pair(
        self,
        connected: &ConnectedLapinBroker,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(self.bind(connected)))
    }
}

impl LapinPublishPolicy for ConfirmsPublish {
    fn bind(self, connected: &ConnectedLapinBroker) -> Self::Live {
        ConfirmsPublisher::new(connected, self.0)
    }
}

/// The server-transactional publish policy: same options as [`LapinPublish`], pairing into
/// [`ServerTxPublisher`].
///
/// Reached with [`LapinPublish::server_tx`].
///
/// # Examples
///
/// ```
/// use ruststream_lapin::LapinPublish;
///
/// let ledger = LapinPublish::default().exchange("ledger").server_tx();
/// # let _ = ledger;
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[must_use]
pub struct ServerTxPublish(PublishOptions);

publish_policy_settings!(ServerTxPublish);

impl PublishPolicy<ConnectedLapinBroker> for ServerTxPublish {
    type Live = ServerTxPublisher;

    fn pair(
        self,
        connected: &ConnectedLapinBroker,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(self.bind(connected)))
    }
}

impl LapinPublishPolicy for ServerTxPublish {
    fn bind(self, connected: &ConnectedLapinBroker) -> Self::Live {
        ServerTxPublisher::new(connected, self.0)
    }
}
