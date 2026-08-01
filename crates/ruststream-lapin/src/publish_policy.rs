//! The declaration half of publishing: the policies and what they pair into.
//!
//! A policy holds nothing but publish options, so it is constructible anywhere - in a router
//! definition, in configuration, before anything connects. Pairing it with a
//! [`ConnectedLapinBroker`] produces the live publisher (see [`crate::publisher`]), which is the
//! only value with a publish surface. The publishing mode is a policy transition:
//! [`LapinPublish::confirms`] and [`LapinPublish::server_tx`] move to the transactional policies,
//! keeping the options.

use ruststream::{PairError, PublishPolicy};

use crate::broker::ConnectedLapinBroker;
use crate::publisher::{ConfirmsPublisher, LapinPublisher, ServerTxPublisher};

use self::sealed::Sealed;

mod sealed {
    /// Seals [`LapinPublishPolicy`](super::LapinPublishPolicy): pairing an AMQP publisher opens
    /// no channel of its own, and the synchronous
    /// [`publisher`](crate::ConnectedLapinBroker::publisher) accessor depends on that.
    pub trait Sealed {}

    impl Sealed for super::LapinPublish {}
    impl Sealed for super::ConfirmsPublish {}
    impl Sealed for super::ServerTxPublish {}
    impl Sealed for crate::requester::LapinRequest {}
}

/// The options every `RabbitMQ` publish policy carries: where to publish and how durably.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PublishOptions {
    pub(crate) exchange: String,
    pub(crate) persistent: bool,
}

impl Default for PublishOptions {
    fn default() -> Self {
        Self {
            exchange: String::new(),
            persistent: true,
        }
    }
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
/// addresses the queue with that name. Messages are published persistent (delivery mode 2)
/// unless [`persistent(false)`](Self::persistent) opts out.
///
/// It pairs into [`LapinPublisher`], and it is the broker's
/// [`DefaultPublish`](ruststream::DefaultPublish) policy, so a `publish("dest")` handler mounted
/// without an explicit publisher replies through it. [`confirms`](Self::confirms) and
/// [`server_tx`](Self::server_tx) move to the transactional policies, keeping the options.
///
/// # Examples
///
/// ```
/// use ruststream_lapin::LapinPublish;
///
/// let events = LapinPublish::default().exchange("events");
/// let shipments = LapinPublish::default().confirms();
/// # let _ = (events, shipments);
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[must_use]
pub struct LapinPublish(PublishOptions);

impl LapinPublish {
    /// Publishes to `exchange` instead of the default exchange.
    pub fn exchange(mut self, exchange: impl Into<String>) -> Self {
        self.0.exchange = exchange.into();
        self
    }

    /// Whether messages are marked persistent (delivery mode 2). Defaults to `true`.
    pub fn persistent(mut self, persistent: bool) -> Self {
        self.0.persistent = persistent;
        self
    }

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

impl PublishPolicy<ConnectedLapinBroker> for LapinPublish {
    type Live = LapinPublisher;

    async fn pair(self, connected: &ConnectedLapinBroker) -> Result<Self::Live, PairError> {
        Ok(self.bind(connected))
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

impl ConfirmsPublish {
    /// Publishes to `exchange` instead of the default exchange.
    pub fn exchange(mut self, exchange: impl Into<String>) -> Self {
        self.0.exchange = exchange.into();
        self
    }

    /// Whether messages are marked persistent (delivery mode 2). Defaults to `true`.
    pub fn persistent(mut self, persistent: bool) -> Self {
        self.0.persistent = persistent;
        self
    }
}

impl PublishPolicy<ConnectedLapinBroker> for ConfirmsPublish {
    type Live = ConfirmsPublisher;

    async fn pair(self, connected: &ConnectedLapinBroker) -> Result<Self::Live, PairError> {
        Ok(self.bind(connected))
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

impl ServerTxPublish {
    /// Publishes to `exchange` instead of the default exchange.
    pub fn exchange(mut self, exchange: impl Into<String>) -> Self {
        self.0.exchange = exchange.into();
        self
    }

    /// Whether messages are marked persistent (delivery mode 2). Defaults to `true`.
    pub fn persistent(mut self, persistent: bool) -> Self {
        self.0.persistent = persistent;
        self
    }
}

impl PublishPolicy<ConnectedLapinBroker> for ServerTxPublish {
    type Live = ServerTxPublisher;

    async fn pair(self, connected: &ConnectedLapinBroker) -> Result<Self::Live, PairError> {
        Ok(self.bind(connected))
    }
}

impl LapinPublishPolicy for ServerTxPublish {
    fn bind(self, connected: &ConnectedLapinBroker) -> Self::Live {
        ServerTxPublisher::new(connected, self.0)
    }
}
