//! The exchange half of a queue binding descriptor.

use lapin::ExchangeKind;

/// Describes the exchange side of a [`RabbitQueue`](crate::RabbitQueue) binding.
///
/// Like every descriptor in this crate it only records the EXPECTED topology; nothing is
/// declared unless the broker was built with
/// [`declare_topology(true)`](crate::LapinBroker::declare_topology).
///
/// # Examples
///
/// ```
/// use ruststream_lapin::RabbitExchange;
///
/// let events = RabbitExchange::topic("events").durable(true);
/// assert_eq!(events.name(), "events");
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RabbitExchange {
    name: String,
    kind: ExchangeKind,
    durable: bool,
    auto_delete: bool,
}

impl RabbitExchange {
    fn new(name: impl Into<String>, kind: ExchangeKind) -> Self {
        Self {
            name: name.into(),
            kind,
            durable: true,
            auto_delete: false,
        }
    }

    /// A direct exchange: routes on an exact routing-key match.
    #[must_use]
    pub fn direct(name: impl Into<String>) -> Self {
        Self::new(name, ExchangeKind::Direct)
    }

    /// A topic exchange: routes on dot-separated routing-key patterns (`order.*`).
    #[must_use]
    pub fn topic(name: impl Into<String>) -> Self {
        Self::new(name, ExchangeKind::Topic)
    }

    /// A fanout exchange: routes every message to every bound queue.
    #[must_use]
    pub fn fanout(name: impl Into<String>) -> Self {
        Self::new(name, ExchangeKind::Fanout)
    }

    /// A headers exchange: routes on header attributes instead of the routing key.
    #[must_use]
    pub fn headers(name: impl Into<String>) -> Self {
        Self::new(name, ExchangeKind::Headers)
    }

    /// An exchange of a plugin-provided type, for example `"x-delayed-message"`.
    #[must_use]
    pub fn custom(name: impl Into<String>, kind: impl Into<String>) -> Self {
        Self::new(name, ExchangeKind::Custom(kind.into()))
    }

    /// A consistent-hash exchange (the `x-consistent-hash` type from the
    /// [`rabbitmq_consistent_hash_exchange`](https://github.com/rabbitmq/rabbitmq-server/tree/main/deps/rabbitmq_consistent_hash_exchange)
    /// plugin): distributes messages across the queues bound to it by hashing the routing key,
    /// so partition-like fan-out is a server-side concern rather than a client one.
    ///
    /// Each queue binds with a routing key that is its integer weight (`"1"`, `"2"`, ...); the
    /// broker splits the hash space proportionally. Requires the plugin to be enabled on the
    /// broker, so it lives behind the `plugin-consistent-hash` feature.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_lapin::{RabbitExchange, RabbitQueue};
    ///
    /// let hashed = RabbitExchange::consistent_hash("orders-by-key");
    /// // Bind a queue with its weight as the routing key:
    /// let shard = RabbitQueue::new("shard-a").bind(hashed, "1");
    /// # let _ = shard;
    /// ```
    #[cfg(feature = "plugin-consistent-hash")]
    #[must_use]
    pub fn consistent_hash(name: impl Into<String>) -> Self {
        Self::new(name, ExchangeKind::Custom("x-consistent-hash".to_owned()))
    }

    /// Whether the exchange survives a broker restart. Defaults to `true`.
    #[must_use]
    pub fn durable(mut self, durable: bool) -> Self {
        self.durable = durable;
        self
    }

    /// Whether the exchange is deleted when its last binding is removed. Defaults to `false`.
    #[must_use]
    pub fn auto_delete(mut self, auto_delete: bool) -> Self {
        self.auto_delete = auto_delete;
        self
    }

    /// The exchange name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn kind(&self) -> &ExchangeKind {
        &self.kind
    }

    pub(crate) fn is_durable(&self) -> bool {
        self.durable
    }

    pub(crate) fn is_auto_delete(&self) -> bool {
        self.auto_delete
    }
}
