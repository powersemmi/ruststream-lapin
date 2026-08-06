//! The broker ladder: [`LapinBroker`] -> [`ConnectedLapinBroker`] -> [`ClosedLapinBroker`].
//!
//! Construction is synchronous and I/O-free; the connection is dialled by the consuming
//! [`Broker::connect`], and the connected form is the only value carrying a subscribe or publish
//! surface. [`ConnectedBroker::shutdown`] consumes it in turn and returns the terminal witness.

use std::num::NonZeroU16;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use lapin::options::{BasicConsumeOptions, BasicQosOptions};
use lapin::types::{FieldTable, ShortString};
use lapin::{Channel, Connection, ConnectionProperties};
use ruststream::{Broker, ConnectedBroker, DefaultPublish, DescribeServer, ServerSpec, Subscribe};

use crate::convert;
use crate::delay::DelayContext;
use crate::error::AmqpError;
use crate::publish_policy::{LapinPublish, LapinPublishPolicy};
use crate::queue::{QueueType, RabbitQueue};
use crate::requester::{LapinRequest, LapinRequester};
use crate::subscriber::LapinSubscriber;
use crate::topology;

/// The live connection plus the shared fire-and-forget publish channel.
///
/// Held behind an [`Arc`] by the connected broker and by every publisher, requester, and
/// subscriber paired off it, so they all speak over the same connection.
pub(crate) struct AmqpConnection {
    connection: Connection,
    publish_channel: Channel,
    closed: AtomicBool,
}

impl AmqpConnection {
    fn new(connection: Connection, publish_channel: Channel) -> Arc<Self> {
        Arc::new(Self {
            connection,
            publish_channel,
            closed: AtomicBool::new(false),
        })
    }

    /// The connection, or [`AmqpError::Closed`] once the broker has shut down.
    ///
    /// Why this stays a runtime check: handles paired before the shutdown alias the connection
    /// and may outlive it, and the typed ladder can only rule out misuse through the owner's
    /// handle.
    pub(crate) fn live_connection(&self, target: &str) -> Result<&Connection, AmqpError> {
        self.ensure_live(target)?;
        Ok(&self.connection)
    }

    /// The shared publish channel, or [`AmqpError::Closed`] once the broker has shut down.
    pub(crate) fn live_publish_channel(&self, target: &str) -> Result<&Channel, AmqpError> {
        self.ensure_live(target)?;
        Ok(&self.publish_channel)
    }

    /// `Ok` while the connection is live, [`AmqpError::Closed`] once the broker has shut down.
    pub(crate) fn ensure_live(&self, target: &str) -> Result<(), AmqpError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(AmqpError::closed(target));
        }
        Ok(())
    }
}

impl std::fmt::Debug for AmqpConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AmqpConnection")
            .field("closed", &self.closed.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

/// A `RabbitMQ` broker backed by [`lapin`](https://docs.rs/lapin): configuration captured, no I/O
/// performed yet.
///
/// [`new`](Self::new) is synchronous and records only the connection settings, so a `RabbitMQ`
/// service is assembled with the synchronous `#[ruststream::app]` builder like any other broker.
/// The runtime calls [`Broker::connect`] once at startup, which consumes this value and yields
/// the [`ConnectedLapinBroker`] witness: subscriptions, publishers, and requesters exist only
/// from there, so "not connected" is not representable.
///
/// By default the broker never creates infrastructure: descriptors describe the EXPECTED
/// topology, and a missing queue is a subscribe error. Opt into declaration with
/// [`declare_topology(true)`](Self::declare_topology).
///
/// # Examples
///
/// ```no_run
/// use ruststream::nonzero;
/// use ruststream_lapin::{LapinBroker, QueueType};
///
/// let broker = LapinBroker::new("amqp://localhost:5672")
///     .prefetch(nonzero!(64))
///     .default_queue_type(QueueType::Quorum);
/// # let _ = broker;
/// ```
#[derive(Debug, Clone)]
#[must_use]
pub struct LapinBroker {
    uri: String,
    connection_name: Option<String>,
    prefetch: Option<NonZeroU16>,
    declare: bool,
    default_queue_type: Option<QueueType>,
}

impl LapinBroker {
    /// Records the connection URI; no I/O happens until [`Broker::connect`].
    ///
    /// The URI carries credentials, virtual host, and TLS scheme:
    /// `amqp://user:pass@host:5672/vhost` (or `amqps://` with a TLS feature enabled).
    pub fn new(uri: impl Into<String>) -> Self {
        Self {
            uri: uri.into(),
            connection_name: None,
            prefetch: None,
            declare: false,
            default_queue_type: None,
        }
    }

    /// A connection name shown in the `RabbitMQ` management UI.
    pub fn connection_name(mut self, name: impl Into<String>) -> Self {
        self.connection_name = Some(name.into());
        self
    }

    /// Caps unacknowledged deliveries in flight per subscription (`basic.qos`).
    ///
    /// This is the back-pressure window for subscriber streams; individual queue descriptors
    /// can override it. Without it the server imposes no prefetch limit.
    ///
    /// The count is a [`NonZeroU16`] because AMQP reads `basic.qos(0)` as "no limit", the exact
    /// opposite of a cap: the zero sentinel is unrepresentable here, and leaving the prefetch
    /// unset is how "unlimited" is spelled.
    pub fn prefetch(mut self, prefetch: NonZeroU16) -> Self {
        self.prefetch = Some(prefetch);
        self
    }

    /// Whether subscribing declares the descriptor's expected topology first. Defaults to
    /// `false`: managing infrastructure is the user's job, so creation is a deliberate opt-in.
    ///
    /// When enabled, subscribing declares the bound exchanges (except the built-in `amq.*`
    /// ones and the default exchange), the queue, and the bindings.
    pub fn declare_topology(mut self, declare: bool) -> Self {
        self.declare = declare;
        self
    }

    /// The queue type declared for descriptors that do not set one.
    ///
    /// Only consulted when [`declare_topology`](Self::declare_topology) is enabled. Without a
    /// broker default or a per-queue type, no `x-queue-type` argument is sent and the server
    /// default applies.
    pub fn default_queue_type(mut self, queue_type: QueueType) -> Self {
        self.default_queue_type = Some(queue_type);
        self
    }
}

impl Broker for LapinBroker {
    type Error = AmqpError;
    type Connected = ConnectedLapinBroker;

    /// Opens the connection and its shared publish channel, consuming the configuration.
    ///
    /// # Errors
    ///
    /// Returns [`AmqpError::Connect`] when the URI cannot be parsed or the connection fails.
    async fn connect(self) -> Result<Self::Connected, Self::Error> {
        let mut properties = ConnectionProperties::default();
        if let Some(name) = &self.connection_name {
            properties = properties.with_connection_name(name.as_str().into());
        }
        let connection = Connection::connect(&self.uri, properties)
            .await
            .map_err(AmqpError::connect)?;
        let publish_channel = connection
            .create_channel()
            .await
            .map_err(AmqpError::connect)?;

        Ok(ConnectedLapinBroker {
            conn: AmqpConnection::new(connection, publish_channel),
            uri: self.uri,
            prefetch: self.prefetch,
            declare: self.declare,
            default_queue_type: self.default_queue_type,
        })
    }
}

/// `DescribeServer` reports the configured AMQP address, which is what the `AsyncAPI` document
/// records for the service.
impl DescribeServer for LapinBroker {
    fn describe_server(&self) -> ServerSpec {
        ServerSpec::new(host_of(&self.uri), "amqp")
    }
}

/// The typed witness that [`Broker::connect`] succeeded: holds the live connection.
///
/// Everything connection-bound hangs off this value: subscriptions ([`Subscribe`],
/// [`subscribe`](Self::subscribe)), publishers ([`publisher`](Self::publisher)), and requesters
/// ([`requester`](Self::requester)). [`ConnectedBroker::shutdown`] consumes it, so a publish or
/// subscribe after shutdown is a compile error for the owner of the handle.
#[derive(Debug)]
pub struct ConnectedLapinBroker {
    conn: Arc<AmqpConnection>,
    uri: String,
    prefetch: Option<NonZeroU16>,
    declare: bool,
    default_queue_type: Option<QueueType>,
}

impl ConnectedLapinBroker {
    pub(crate) fn connection(&self) -> &Arc<AmqpConnection> {
        &self.conn
    }

    /// The `AsyncAPI` server description of the connection this broker dialled.
    #[must_use]
    pub fn server_spec(&self) -> ServerSpec {
        ServerSpec::new(host_of(&self.uri), "amqp")
    }

    /// Opens a subscription for `def`, declaring its topology first when the broker opted in.
    ///
    /// # Errors
    ///
    /// Returns [`AmqpError::Closed`] after shutdown, [`AmqpError::Declare`] when opted-in
    /// declaration fails, [`AmqpError::InvalidOptions`] for contradictory descriptor options,
    /// and [`AmqpError::Subscribe`] when the channel or consumer cannot be opened (for example
    /// the queue does not exist and declaration was not opted into).
    pub async fn subscribe(&self, def: RabbitQueue) -> Result<LapinSubscriber, AmqpError> {
        let channel = self
            .conn
            .live_connection(def.name())?
            .create_channel()
            .await
            .map_err(AmqpError::subscribe)?;

        if self.declare {
            topology::declare(&channel, &def, self.default_queue_type).await?;
        }
        if let Some(prefetch) = def.prefetch_or(self.prefetch) {
            channel
                .basic_qos(prefetch.get(), BasicQosOptions::default())
                .await
                .map_err(AmqpError::subscribe)?;
        }

        let queue = def.name().to_owned();
        // A native delay backend re-publishes the delayed copy on the same channel the delivery is
        // acked on, so no extra channel is created and the publish orders naturally before the
        // ack (duplicate-not-loss).
        let delay = def
            .delay_config()
            .map(|delay| DelayContext::new(channel.clone(), delay.target_for(&queue)));

        let consumer = channel
            .basic_consume(
                convert::short(&queue, "queue name")?,
                ShortString::default(),
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .map_err(AmqpError::subscribe)?;

        Ok(LapinSubscriber::new(channel, consumer, queue, delay))
    }

    /// A live publisher for `policy`.
    ///
    /// [`LapinPublish`] pairs into the fire-and-forget publisher,
    /// [`ConfirmsPublish`](crate::ConfirmsPublish) into the confirm-transactional one, and
    /// [`ServerTxPublish`](crate::ServerTxPublish) into the AMQP-server-transactional one. All
    /// three are cheap to build and cheap to clone.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use ruststream::Broker;
    /// use ruststream_lapin::{LapinBroker, LapinPublish};
    ///
    /// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
    /// let connected = LapinBroker::new("amqp://localhost:5672").connect().await?;
    /// let orders = connected.publisher(LapinPublish::default().exchange("orders"));
    /// let shipments = connected.publisher(LapinPublish::default().confirms());
    /// # let _ = (orders, shipments);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn publisher<P: LapinPublishPolicy>(&self, policy: P) -> P::Live {
        policy.bind(self)
    }

    /// A live request/reply client over `RabbitMQ` direct reply-to.
    ///
    /// The requester half of [`LapinRequest`]; [`publisher`](Self::publisher) accepts the same
    /// policy, this accessor only names the result.
    #[must_use]
    pub fn requester(&self, policy: LapinRequest) -> LapinRequester {
        policy.bind(self)
    }
}

impl ConnectedBroker for ConnectedLapinBroker {
    type Error = AmqpError;
    type Closed = ClosedLapinBroker;

    /// Closes the connection, consuming the connected form.
    ///
    /// # Errors
    ///
    /// Returns [`AmqpError::Connect`] when the close handshake fails.
    async fn shutdown(self) -> Result<Self::Closed, Self::Error> {
        // Marked closed before the handshake: a publisher aliasing the connection must not slip
        // a message into a connection that is already going away.
        self.conn.closed.store(true, Ordering::Release);
        let handshake = self.conn.connection.status().connected();
        if handshake {
            self.conn
                .connection
                .close(200, ShortString::from("OK"))
                .await
                .map_err(AmqpError::connect)?;
        }
        Ok(ClosedLapinBroker { handshake })
    }
}

// By-name subscription capability: the runtime's default `Name` source resolves through this for
// the bare-string `#[subscriber("queue")]` form.
//
// `Self::subscribe` inside this impl would resolve to the trait method and recurse; the type
// name is the only way to reach the inherent one.
#[allow(clippy::use_self)]
impl Subscribe for ConnectedLapinBroker {
    type Subscriber = LapinSubscriber;

    /// Subscribes to the queue `name` with descriptor defaults (durable, shared).
    async fn subscribe(&self, name: &str) -> Result<Self::Subscriber, Self::Error> {
        ConnectedLapinBroker::subscribe(self, RabbitQueue::new(name)).await
    }
}

impl DefaultPublish for ConnectedLapinBroker {
    type Policy = LapinPublish;
}

/// The terminal witness returned by shutting down a [`ConnectedLapinBroker`].
///
/// It has no publish or subscribe surface; it carries whether the close handshake actually ran,
/// which distinguishes an orderly teardown from closing a connection the server had already
/// dropped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClosedLapinBroker {
    handshake: bool,
}

impl ClosedLapinBroker {
    /// Whether the AMQP close handshake ran, as opposed to the connection already being down.
    #[must_use]
    pub const fn handshake(&self) -> bool {
        self.handshake
    }
}

/// Extracts the `host[:port]` part of an AMQP URI for `AsyncAPI` metadata; never fails, because
/// metadata must not block startup on a URI the connection itself will reject anyway.
fn host_of(uri: &str) -> String {
    let after_scheme = uri.split_once("://").map_or(uri, |(_, rest)| rest);
    let after_auth = after_scheme
        .rsplit_once('@')
        .map_or(after_scheme, |(_, rest)| rest);
    let host = after_auth.split(['/', '?']).next().unwrap_or(after_auth);
    host.to_owned()
}

#[cfg(test)]
mod tests {
    use super::{DescribeServer, LapinBroker, host_of};

    #[test]
    fn host_extraction_handles_auth_vhost_and_bare_forms() {
        assert_eq!(host_of("amqp://localhost:5672"), "localhost:5672");
        assert_eq!(host_of("amqp://user:pass@rabbit:5672/prod"), "rabbit:5672");
        assert_eq!(host_of("amqps://rabbit/vhost"), "rabbit");
        assert_eq!(host_of("rabbit:5672"), "rabbit:5672");
    }

    // `new` records the settings without connecting: no server is needed to build the broker or
    // to describe it, which is what lets it slot into the synchronous app builder.
    #[test]
    fn new_performs_no_io_and_describes_the_configured_address() {
        let spec = LapinBroker::new("amqp://127.0.0.1:5672").describe_server();
        assert_eq!(spec.protocol, "amqp");
        assert_eq!(spec.host.as_deref(), Some("127.0.0.1:5672"));
    }
}
