//! The broker ladder: [`LapinBroker`] -> [`ConnectedLapinBroker`] -> [`ClosedLapinBroker`].
//!
//! Construction is synchronous and I/O-free; the connection is dialled by the consuming
//! [`Broker::connect`], and the connected form is the only value carrying a subscribe or publish
//! surface. [`ConnectedBroker::shutdown`] consumes it in turn and returns the terminal witness.

// Without the `testing` feature a connection link has one variant, so a `match` on it has a
// single arm; the matches stay so that the in-process arm has its place when the feature is on.
#![cfg_attr(
    not(feature = "testing"),
    allow(clippy::infallible_destructuring_match)
)]

#[cfg(feature = "testing")]
use std::future::{Future, ready};
use std::num::NonZeroU16;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use lapin::options::{BasicConsumeOptions, BasicQosOptions};
use lapin::types::{FieldTable, ShortString};
#[cfg(feature = "testing")]
use lapin::uri::AMQPUri;
use lapin::{Channel, ChannelState, Connection, ConnectionProperties, ConnectionState, ErrorKind};
#[cfg(feature = "testing")]
use ruststream::testing::{Coordinator, InProcess, TestableBroker};
use ruststream::{
    AddressedCopies, Broker, ConnectedBroker, DefaultPublish, DescribeServer, ServerSpec, Subscribe,
};
#[cfg(feature = "testing")]
use ruststream::{OutgoingMessage, RawMessage};

use crate::channel::ChannelCell;
use crate::convert;
use crate::delay::DelayContext;
use crate::error::AmqpError;
#[cfg(feature = "testing")]
use crate::in_process::{self, Bus};
use crate::publish_policy::{LapinPublish, LapinPublishPolicy};
#[cfg(feature = "testing")]
use crate::publish_step::LapinPublishOptions;
use crate::queue::{QueueDescriptor, RabbitQueue, declared_arguments};
use crate::requester::{LapinRequest, LapinRequester};
use crate::subscriber::LapinSubscriber;
use crate::topology;

/// The protocol name the `AsyncAPI` document reports for this crate.
pub(crate) const PROTOCOL: &str = "amqp";

/// The wire version behind that name. `RabbitMQ` speaks AMQP 0.9.1, and AMQP 1.0 is a different
/// protocol with a binding key of its own, so the version is what tells a reader which one a
/// client has to speak here.
pub(crate) const PROTOCOL_VERSION: &str = "0.9.1";

/// The live connection plus the shared fire-and-forget publish channel.
///
/// Held behind an [`Arc`] by the connected broker and by every publisher, requester, and
/// subscriber paired off it, so they all speak over the same connection.
pub(crate) struct AmqpConnection {
    connection: Connection,
    publish_channel: ChannelCell<Channel>,
    closed: AtomicBool,
}

impl AmqpConnection {
    fn new(connection: Connection, publish_channel: Channel) -> Arc<Self> {
        Arc::new(Self {
            connection,
            publish_channel: ChannelCell::holding(publish_channel),
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
    ///
    /// Opened again when the one held here is gone. A channel-level error closes the channel and
    /// leaves the connection up - publishing to an exchange that does not exist is enough, and
    /// the fire-and-forget publisher does not see that answer - so a channel kept for the
    /// connection's lifetime would turn one mistyped exchange into a service that never publishes
    /// again until it is restarted.
    pub(crate) async fn live_publish_channel(&self, target: &str) -> Result<Channel, AmqpError> {
        self.ensure_live(target)?;
        self.publish_channel
            .get(|| async {
                self.connection
                    .create_channel()
                    .await
                    .map_err(AmqpError::publish)
            })
            .await
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

/// What a connected broker and every handle paired off it speak over: the live connection, or,
/// under the `testing` feature, the in-process transport the test harness connected instead.
///
/// Without the feature there is one variant, so the type is the connection handle itself and
/// every `match` on it is irrefutable: a production build carries no second transport and no
/// branch to it.
#[derive(Debug, Clone)]
pub(crate) enum Link {
    Amqp(Arc<AmqpConnection>),
    #[cfg(feature = "testing")]
    InProcess(Arc<Bus>),
}

// The zero-cost promise of the in-process mode, held by the compiler: a build without it gives the
// link exactly the size of the connection handle it wraps.
#[cfg(not(feature = "testing"))]
const _: () = assert!(size_of::<Link>() == size_of::<Arc<AmqpConnection>>());

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
/// use ruststream_lapin::LapinBroker;
///
/// let broker = LapinBroker::new("amqp://localhost:5672")
///     .prefetch(nonzero!(64))
///     .declare_topology(true);
/// # let _ = broker;
/// ```
#[derive(Debug, Clone)]
#[must_use]
pub struct LapinBroker {
    uri: String,
    connection_name: Option<String>,
    prefetch: Option<NonZeroU16>,
    declare: bool,
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
            link: Link::Amqp(AmqpConnection::new(connection, publish_channel)),
            uri: self.uri,
            prefetch: self.prefetch,
            declare: self.declare,
        })
    }
}

/// The in-process mode: the connected form a test runs the production app against, carrying the
/// in-process transport in place of the connection and every setting of this broker.
///
/// The URI is parsed as `connect` parses it, so a broker a service could not connect is not one a
/// test can connect either.
#[cfg(feature = "testing")]
impl InProcess for LapinBroker {
    fn connect_in_process(
        self,
    ) -> impl Future<Output = Result<Self::Connected, Self::Error>> + Send {
        let connected = self
            .uri
            .parse::<AMQPUri>()
            .map_err(|err| AmqpError::Connect(err.into()))
            .map(|_| ConnectedLapinBroker {
                link: Link::InProcess(Bus::new()),
                uri: self.uri,
                prefetch: self.prefetch,
                declare: self.declare,
            });
        ready(connected)
    }
}

#[cfg(feature = "testing")]
ruststream::register_testable_broker!(LapinBroker);

/// `DescribeServer` reports the configured AMQP address, which is what the `AsyncAPI` document
/// records for the service.
///
/// The document is published and shared, so the credentials an AMQP URI carries must not reach it.
/// `ServerSpec::from_url` drops them, along with the scheme and the vhost path.
impl DescribeServer for LapinBroker {
    fn describe_server(&self) -> ServerSpec {
        ServerSpec::from_url(&self.uri, PROTOCOL).protocol_version(PROTOCOL_VERSION)
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
    link: Link,
    uri: String,
    prefetch: Option<NonZeroU16>,
    declare: bool,
}

impl ConnectedLapinBroker {
    /// What every handle paired off this broker speaks over.
    pub(crate) fn link(&self) -> &Link {
        &self.link
    }

    /// The `AsyncAPI` server description of the connection this broker dialled.
    #[must_use]
    pub fn server_spec(&self) -> ServerSpec {
        ServerSpec::from_url(&self.uri, PROTOCOL).protocol_version(PROTOCOL_VERSION)
    }

    /// Opens a subscription for `def`, declaring its topology first when the broker opted in.
    ///
    /// Takes either queue descriptor: [`RabbitQueue`] or
    /// [`RabbitQuorumQueue`](crate::RabbitQuorumQueue).
    ///
    /// # Errors
    ///
    /// Returns [`AmqpError::Closed`] after shutdown, [`AmqpError::Declare`] when opted-in
    /// declaration fails, [`AmqpError::InvalidOptions`] for contradictory descriptor options and
    /// for a retry declaration this queue cannot carry, and [`AmqpError::Subscribe`] when the
    /// channel or consumer cannot be opened (for example the queue does not exist and declaration
    /// was not opted into).
    pub async fn subscribe(&self, def: impl QueueDescriptor) -> Result<LapinSubscriber, AmqpError> {
        let conn = match &self.link {
            Link::Amqp(conn) => conn,
            #[cfg(feature = "testing")]
            Link::InProcess(bus) => return in_process::subscribe(bus, &def, self.declare),
        };
        let spec = def.spec();
        // Before the channel: a declaration the queue cannot carry is the registration's mistake,
        // and the service should not reach a live consumer with it.
        def.check_retry(self.declare)?;

        let channel = conn
            .live_connection(&spec.name)?
            .create_channel()
            .await
            .map_err(AmqpError::subscribe)?;

        if self.declare {
            let arguments = declared_arguments(spec, def.declared_retry());
            topology::declare(&channel, spec, arguments).await?;
        }
        if let Some(prefetch) = spec.prefetch_or(self.prefetch) {
            channel
                .basic_qos(prefetch.get(), BasicQosOptions::default())
                .await
                .map_err(AmqpError::subscribe)?;
        }

        let queue = spec.name.clone();
        // A native delay backend re-publishes the delayed copy on the same channel the delivery is
        // acked on, so no extra channel is created and the publish orders naturally before the
        // ack (duplicate-not-loss).
        let delay = spec
            .delay
            .as_ref()
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

        Ok(LapinSubscriber::new(
            channel,
            consumer,
            queue,
            spec.batch_wait,
            delay,
        ))
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
        let conn = match self.link {
            Link::Amqp(conn) => conn,
            #[cfg(feature = "testing")]
            Link::InProcess(bus) => {
                bus.close();
                return Ok(ClosedLapinBroker { handshake: true });
            }
        };
        // Marked closed before the handshake: a publisher aliasing the connection must not slip
        // a message into a connection that is already going away.
        conn.closed.store(true, Ordering::Release);
        let handshake = conn.connection.status().connected();
        if handshake {
            match conn.connection.close(200, ShortString::from("OK")).await {
                Ok(()) => {}
                // A subscriber dropped just before the shutdown is still cancelling its consumer,
                // and lapin refuses to close a channel it is already closing. The connection is
                // going away either way, so reporting that as a teardown failure would make an
                // orderly shutdown look broken - and would do so only sometimes, which is worse.
                Err(err) if already_closing(&err) => {}
                Err(err) => return Err(AmqpError::connect(err)),
            }
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

    /// A queue name is an address as well as a subscription: on the default exchange a routing key
    /// addresses the queue that carries it, so the service publishes a delayed redelivery back
    /// under the name it subscribed to.
    ///
    /// The copy reaches the queue as long as the retry publisher sends on the default exchange,
    /// which is what [`LapinPublish`] does unless [`exchange`](LapinPublish::exchange) says
    /// otherwise; pointing that publisher at a topic exchange with no binding under the queue name
    /// would send the copy nowhere, so bind it there or leave the retry publisher on the default
    /// exchange.
    type Copies = AddressedCopies;

    /// Subscribes to the queue `name` with descriptor defaults (durable, shared).
    async fn subscribe(&self, name: &str) -> Result<Self::Subscriber, Self::Error> {
        ConnectedLapinBroker::subscribe(self, RabbitQueue::new(name)).await
    }

    // `declare_retry` keeps the default, which accepts the declaration and leaves the runtime to
    // apply it. A cap RabbitMQ enforces itself is `x-delivery-limit` with a dead-letter route, and
    // a queue takes both at declaration time: a name arriving here is a queue that already exists,
    // so there is nothing to declare them on. `RabbitQuorumQueue` is where they become topology.
}

impl DefaultPublish for ConnectedLapinBroker {
    type Policy = LapinPublish;
}

/// The harness's view of the in-process transport: what it injects, what it reads back, and the
/// coordinator it counts the in-flight deliveries with.
///
/// # Panics
///
/// `inject` and `published` panic on a broker connected with `connect`: the harness drives only
/// the connection `connect_in_process` produced, and a live connection has no log to read and no
/// synchronous way to take a message.
#[cfg(feature = "testing")]
impl TestableBroker for ConnectedLapinBroker {
    fn install_coordinator(&self, coordinator: Coordinator) {
        if let Link::InProcess(bus) = &self.link {
            bus.install(coordinator);
        }
    }

    fn inject(&self, message: OutgoingMessage<'_>) {
        let bus = self.bus("inject");
        // An external producer publishes on the default exchange, the name addressing the queue,
        // with nothing but its headers to frame.
        if let Err(err) = bus.publish(
            "",
            message.name(),
            message.payload(),
            message.headers(),
            &LapinPublishOptions::default(),
        ) {
            panic!(
                "the injected message to {:?} is not one the server takes: {err}",
                message.name()
            );
        }
    }

    fn published(&self, name: &str) -> Vec<RawMessage> {
        self.bus("published").published(name)
    }
}

#[cfg(feature = "testing")]
impl ConnectedLapinBroker {
    /// The in-process transport, which is all the harness drives.
    fn bus(&self, what: &str) -> &Arc<Bus> {
        match &self.link {
            Link::InProcess(bus) => bus,
            Link::Amqp(_) => panic!(
                "TestableBroker::{what} reached a broker connected with `connect`; the harness \
                 drives the connection `connect_in_process` produces"
            ),
        }
    }
}

/// Whether this error says the close is already under way rather than that it failed.
fn already_closing(err: &lapin::Error) -> bool {
    matches!(
        err.kind(),
        ErrorKind::InvalidChannelState(ChannelState::Closing | ChannelState::Closed, _)
            | ErrorKind::InvalidConnectionState(ConnectionState::Closing | ConnectionState::Closed)
    )
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

#[cfg(test)]
mod tests {
    use super::{DescribeServer, LapinBroker};

    fn described_host(uri: &str) -> String {
        LapinBroker::new(uri)
            .describe_server()
            .host
            .expect("an AMQP URI always names a host")
    }

    // The published document must carry the coordinate and nothing else, whatever an operator put
    // in the connection URI: no password, no vhost.
    #[test]
    fn the_description_is_the_host_alone_whatever_the_uri_carries() {
        assert_eq!(described_host("amqp://localhost:5672"), "localhost:5672");
        assert_eq!(
            described_host("amqp://user:pass@rabbit:5672/prod"),
            "rabbit:5672"
        );
        assert_eq!(described_host("amqps://rabbit/vhost"), "rabbit");
        assert_eq!(described_host("rabbit:5672"), "rabbit:5672");
        // A vhost may contain an `@`, so the path is cut before the userinfo is.
        assert_eq!(described_host("amqp://rabbit:5672/my@vhost"), "rabbit:5672");
        // Both an `@` in the userinfo and one in the vhost: each is cut at its own step.
        assert_eq!(
            described_host("amqp://user:p@ss@rabbit:5672/my@vhost"),
            "rabbit:5672"
        );
        assert_eq!(
            described_host("amqp://rabbit:5672/prod?heartbeat=30"),
            "rabbit:5672"
        );
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
