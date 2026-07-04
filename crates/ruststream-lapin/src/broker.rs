//! The broker handle: connection lifecycle, subscriptions, and publisher constructors.

use std::sync::Arc;

use lapin::options::{
    BasicConsumeOptions, BasicQosOptions, ExchangeDeclareOptions, QueueBindOptions,
    QueueDeclareOptions,
};
use lapin::types::{AMQPValue, FieldTable, ShortString};
use lapin::{Channel, Connection, ConnectionProperties};
use ruststream::{Broker, DescribeServer, ServerSpec, Subscribe};
use tokio::sync::OnceCell;

use crate::convert;
use crate::delay::DelayContext;
use crate::error::AmqpError;
use crate::publisher::LapinPublisher;
use crate::queue::{QueueType, RabbitQueue};
use crate::requester::LapinRequester;
use crate::subscriber::LapinSubscriber;

/// The live connection plus the shared fire-and-forget publish channel.
#[derive(Debug)]
pub(crate) struct ConnState {
    connection: Connection,
    publish_channel: Channel,
}

impl ConnState {
    pub(crate) fn connection(&self) -> &Connection {
        &self.connection
    }

    pub(crate) fn publish_channel(&self) -> &Channel {
        &self.publish_channel
    }
}

/// The connection cell shared by the broker and everything it hands out, so publishers obtained
/// before `Broker::connect` resolve the connection on first use.
pub(crate) type SharedConn = Arc<OnceCell<ConnState>>;

/// A `RabbitMQ` broker backed by [`lapin`](https://docs.rs/lapin).
///
/// Follows the `RustStream` lazy startup contract: [`new`](Self::new) is synchronous and does no
/// I/O; the network work happens in the idempotent async `Broker::connect`, which the runtime
/// calls once at startup. Publishers handed out earlier share the connection cell and resolve it
/// on first use.
///
/// By default the broker never creates infrastructure: descriptors describe the EXPECTED
/// topology, and a missing queue is a subscribe error. Opt into declaration with
/// [`declare_topology(true)`](Self::declare_topology).
///
/// # Examples
///
/// ```no_run
/// use ruststream_lapin::{LapinBroker, QueueType};
///
/// let broker = LapinBroker::new("amqp://localhost:5672")
///     .prefetch(64)
///     .default_queue_type(QueueType::Quorum);
/// # let _ = broker;
/// ```
#[derive(Debug, Clone)]
pub struct LapinBroker {
    conn: SharedConn,
    uri: String,
    connection_name: Option<String>,
    prefetch: Option<u16>,
    declare: bool,
    default_queue_type: Option<QueueType>,
}

impl LapinBroker {
    /// Records the connection URI; no I/O happens until `Broker::connect`.
    ///
    /// The URI carries credentials, virtual host, and TLS scheme:
    /// `amqp://user:pass@host:5672/vhost` (or `amqps://` with a TLS feature enabled).
    #[must_use]
    pub fn new(uri: impl Into<String>) -> Self {
        Self {
            conn: Arc::new(OnceCell::new()),
            uri: uri.into(),
            connection_name: None,
            prefetch: None,
            declare: false,
            default_queue_type: None,
        }
    }

    /// Connects eagerly: [`new`](Self::new) followed by `Broker::connect`.
    ///
    /// # Errors
    ///
    /// Returns [`AmqpError::Connect`] when the connection cannot be established.
    pub async fn connect(uri: impl Into<String>) -> Result<Self, AmqpError> {
        let broker = Self::new(uri);
        Broker::connect(&broker).await?;
        Ok(broker)
    }

    /// A connection name shown in the `RabbitMQ` management UI.
    #[must_use]
    pub fn connection_name(mut self, name: impl Into<String>) -> Self {
        self.connection_name = Some(name.into());
        self
    }

    /// Caps unacknowledged deliveries in flight per subscription (`basic.qos`).
    ///
    /// This is the back-pressure window for subscriber streams; individual queue descriptors
    /// can override it. Without it the server imposes no prefetch limit.
    #[must_use]
    pub fn prefetch(mut self, prefetch: u16) -> Self {
        self.prefetch = Some(prefetch);
        self
    }

    /// Whether subscribing declares the descriptor's expected topology first. Defaults to
    /// `false`: managing infrastructure is the user's job, so creation is a deliberate opt-in.
    ///
    /// When enabled, subscribing declares the bound exchanges (except the built-in `amq.*`
    /// ones and the default exchange), the queue, and the bindings.
    #[must_use]
    pub fn declare_topology(mut self, declare: bool) -> Self {
        self.declare = declare;
        self
    }

    /// The queue type declared for descriptors that do not set one.
    ///
    /// Only consulted when [`declare_topology`](Self::declare_topology) is enabled. Without a
    /// broker default or a per-queue type, no `x-queue-type` argument is sent and the server
    /// default applies.
    #[must_use]
    pub fn default_queue_type(mut self, queue_type: QueueType) -> Self {
        self.default_queue_type = Some(queue_type);
        self
    }

    fn connected(&self) -> Result<&ConnState, AmqpError> {
        self.conn.get().ok_or(AmqpError::NotConnected)
    }

    /// Opens a subscription for `def`, declaring its topology first when the broker opted in.
    ///
    /// # Errors
    ///
    /// Returns [`AmqpError::NotConnected`] before `Broker::connect`, [`AmqpError::Declare`] when
    /// opted-in declaration fails, [`AmqpError::InvalidOptions`] for contradictory descriptor
    /// options, and [`AmqpError::Subscribe`] when the channel or consumer cannot be opened (for
    /// example the queue does not exist and declaration was not opted into).
    pub async fn subscribe(&self, def: RabbitQueue) -> Result<LapinSubscriber, AmqpError> {
        let state = self.connected()?;
        let channel = state
            .connection
            .create_channel()
            .await
            .map_err(AmqpError::subscribe)?;

        if self.declare {
            declare_topology(&channel, &def, self.default_queue_type).await?;
        }
        if let Some(prefetch) = def.prefetch_or(self.prefetch) {
            channel
                .basic_qos(prefetch, BasicQosOptions::default())
                .await
                .map_err(AmqpError::subscribe)?;
        }

        let queue = def.name().to_owned();
        // A native delay queue re-publishes the delayed copy on the same channel the delivery is
        // acked on, so no extra channel is created and the publish orders naturally before the
        // ack (duplicate-not-loss).
        let delay = def
            .delay_config()
            .map(|delay| DelayContext::new(channel.clone(), delay.waiting_queue_for(&queue)));

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

    /// A fire-and-forget publisher on the shared publish channel.
    ///
    /// Upgrade with [`confirms`](LapinPublisher::confirms) or
    /// [`server_tx`](LapinPublisher::server_tx) for transactional publishing.
    #[must_use]
    pub fn publisher(&self) -> LapinPublisher {
        LapinPublisher::new(Arc::clone(&self.conn))
    }

    /// A request/reply client over `RabbitMQ` direct reply-to.
    #[must_use]
    pub fn requester(&self) -> LapinRequester {
        LapinRequester::new(Arc::clone(&self.conn))
    }
}

impl Broker for LapinBroker {
    type Error = AmqpError;

    /// Establishes the connection and the shared publish channel; idempotent.
    ///
    /// # Errors
    ///
    /// Returns [`AmqpError::Connect`] when the URI cannot be parsed or the connection fails.
    async fn connect(&self) -> Result<(), Self::Error> {
        self.conn
            .get_or_try_init(|| async {
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
                Ok(ConnState {
                    connection,
                    publish_channel,
                })
            })
            .await?;
        Ok(())
    }

    /// Closes the connection; further operations fail with [`AmqpError::NotConnected`] or a
    /// channel error. Idempotent: closing an already-closed connection succeeds.
    ///
    /// # Errors
    ///
    /// Returns [`AmqpError::Connect`] when the close handshake fails.
    async fn shutdown(&self) -> Result<(), Self::Error> {
        if let Some(state) = self.conn.get()
            && state.connection.status().connected()
        {
            state
                .connection
                .close(200, ShortString::from("OK"))
                .await
                .map_err(AmqpError::connect)?;
        }
        Ok(())
    }
}

// `Self::subscribe` inside this impl would resolve to the trait method and recurse; the type
// name is the only way to reach the inherent one.
#[allow(clippy::use_self)]
impl Subscribe for LapinBroker {
    type Subscriber = LapinSubscriber;

    /// Subscribes to the queue `name` with descriptor defaults (durable, shared).
    async fn subscribe(&self, name: &str) -> Result<Self::Subscriber, Self::Error> {
        LapinBroker::subscribe(self, RabbitQueue::new(name)).await
    }
}

impl DescribeServer for LapinBroker {
    fn describe_server(&self) -> ServerSpec {
        ServerSpec::new(host_of(&self.uri), "amqp")
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

async fn declare_topology(
    channel: &Channel,
    def: &RabbitQueue,
    broker_default: Option<QueueType>,
) -> Result<(), AmqpError> {
    for (exchange, _) in def.bindings() {
        // The default exchange and the amq.* built-ins exist on every broker and must not be
        // redeclared.
        if exchange.name().is_empty() || exchange.name().starts_with("amq.") {
            continue;
        }
        channel
            .exchange_declare(
                convert::short(exchange.name(), "exchange name")?,
                exchange.kind().clone(),
                ExchangeDeclareOptions {
                    durable: exchange.is_durable(),
                    auto_delete: exchange.is_auto_delete(),
                    ..ExchangeDeclareOptions::default()
                },
                FieldTable::default(),
            )
            .await
            .map_err(AmqpError::declare)?;
    }

    let queue_type = def.queue_type_or(broker_default);
    if queue_type == Some(QueueType::Quorum) && !def.is_durable() {
        return Err(AmqpError::InvalidOptions(format!(
            "queue {:?} is a quorum queue and must stay durable; drop `.durable(false)` or pick \
             `QueueType::Classic`",
            def.name(),
        )));
    }

    let mut arguments = def.declare_arguments().clone();
    if let Some(queue_type) = queue_type {
        arguments.insert(
            ShortString::from("x-queue-type"),
            AMQPValue::LongString(queue_type.as_str().into()),
        );
    }
    channel
        .queue_declare(
            convert::short(def.name(), "queue name")?,
            QueueDeclareOptions {
                durable: def.is_durable(),
                exclusive: def.is_exclusive(),
                auto_delete: def.is_auto_delete(),
                ..QueueDeclareOptions::default()
            },
            arguments,
        )
        .await
        .map_err(AmqpError::declare)?;

    for (exchange, routing_key) in def.bindings() {
        channel
            .queue_bind(
                convert::short(def.name(), "queue name")?,
                convert::short(exchange.name(), "exchange name")?,
                convert::short(routing_key, "routing key")?,
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .map_err(AmqpError::declare)?;
    }

    if let Some(delay) = def.delay_config() {
        declare_delay_queue(channel, delay.waiting_queue_for(def.name()), def.name()).await?;
    }

    Ok(())
}

/// Declares the delay waiting queue: durable, with a per-message TTL applied by the sender and a
/// dead-letter route back to `origin` on the default exchange (so an expired message returns to
/// the queue it came from).
async fn declare_delay_queue(
    channel: &Channel,
    waiting_queue: String,
    origin: &str,
) -> Result<(), AmqpError> {
    let mut arguments = FieldTable::default();
    arguments.insert(
        ShortString::from("x-dead-letter-exchange"),
        AMQPValue::LongString(String::new().into()),
    );
    arguments.insert(
        ShortString::from("x-dead-letter-routing-key"),
        AMQPValue::LongString(origin.into()),
    );
    channel
        .queue_declare(
            convert::short(&waiting_queue, "waiting queue name")?,
            QueueDeclareOptions {
                durable: true,
                ..QueueDeclareOptions::default()
            },
            arguments,
        )
        .await
        .map_err(AmqpError::declare)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::host_of;

    #[test]
    fn host_extraction_handles_auth_vhost_and_bare_forms() {
        assert_eq!(host_of("amqp://localhost:5672"), "localhost:5672");
        assert_eq!(host_of("amqp://user:pass@rabbit:5672/prod"), "rabbit:5672");
        assert_eq!(host_of("amqps://rabbit/vhost"), "rabbit");
        assert_eq!(host_of("rabbit:5672"), "rabbit:5672");
    }
}
