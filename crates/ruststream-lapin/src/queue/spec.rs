//! What both queue descriptors carry, and the arguments a declaration turns it into.

use std::num::NonZeroU16;
use std::time::Duration;

use lapin::types::{AMQPValue, FieldTable, ShortString};
use ruststream::RetryDeclaration;

use crate::delay::Delay;
use crate::exchange::RabbitExchange;

/// How long a partial batch waits for more deliveries before it goes to the handler, unless the
/// descriptor's `batch_wait` says otherwise.
///
/// Fifty milliseconds is a compromise for a network transport: long enough for the broker to
/// push the rest of a prefetch window across the connection (many round trips on any healthy
/// link), short enough to bound how long the tail of a backlog sits unhandled.
const DEFAULT_BATCH_WAIT: Duration = Duration::from_millis(50);

/// The queue implementation a declaration asks the server for.
pub(crate) const QUEUE_TYPE: &str = "x-queue-type";

/// The exchange a rejected or spent delivery is re-published to.
pub(crate) const DEAD_LETTER_EXCHANGE: &str = "x-dead-letter-exchange";

/// The routing key it carries there, instead of the one it arrived with.
pub(crate) const DEAD_LETTER_ROUTING_KEY: &str = "x-dead-letter-routing-key";

/// How often a quorum queue returns a message before it dead-letters it.
pub(crate) const DELIVERY_LIMIT: &str = "x-delivery-limit";

/// Which of the two implementations a descriptor describes.
///
/// The type of a queue is fixed when it is created and can never change, so it is a property of
/// the descriptor type rather than a setting on it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum QueueKind {
    Classic,
    Quorum,
}

impl QueueKind {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Classic => "classic",
            Self::Quorum => "quorum",
        }
    }
}

/// The settings a queue carries whichever implementation it is.
///
/// Reached through [`QueueDescriptor`](super::QueueDescriptor), which the broker reads a
/// descriptor with; machinery, never named directly.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq)]
pub struct QueueSpec {
    pub(crate) kind: QueueKind,
    pub(crate) name: String,
    pub(crate) durable: bool,
    pub(crate) exclusive: bool,
    pub(crate) auto_delete: bool,
    pub(crate) bindings: Vec<(RabbitExchange, String)>,
    pub(crate) arguments: FieldTable,
    pub(crate) prefetch: Option<NonZeroU16>,
    pub(crate) batch_wait: Duration,
    pub(crate) delay: Option<Delay>,
}

impl QueueSpec {
    /// A queue of `kind` named `name`, with the defaults a descriptor of that kind starts from.
    pub(crate) fn new(kind: QueueKind, name: String) -> Self {
        Self {
            kind,
            name,
            // A quorum queue is durable, shared and permanent by definition, and those are the
            // classic queue's defaults too, so one set of defaults serves both.
            durable: true,
            exclusive: false,
            auto_delete: false,
            bindings: Vec::new(),
            arguments: FieldTable::default(),
            prefetch: None,
            batch_wait: DEFAULT_BATCH_WAIT,
            delay: None,
        }
    }

    pub(crate) fn prefetch_or(&self, broker_default: Option<NonZeroU16>) -> Option<NonZeroU16> {
        self.prefetch.or(broker_default)
    }

    pub(crate) fn set(&mut self, name: impl Into<String>, value: AMQPValue) {
        self.arguments.insert(ShortString::from(name.into()), value);
    }
}

/// The argument table a declaration of `spec` sends, with what the mount site declared written
/// over the descriptor's own settings.
///
/// The registration is the more specific statement about the retries of the handler mounted on
/// it, so `max_attempts(n).dead_letter(..)` wins over a `delivery_limit(n)` on the descriptor.
pub(crate) fn declared_arguments(spec: &QueueSpec, retry: Option<&RetryDeclaration>) -> FieldTable {
    let mut arguments = spec.arguments.clone();
    arguments.insert(
        ShortString::from(QUEUE_TYPE),
        AMQPValue::LongString(spec.kind.as_str().into()),
    );
    if let Some(declared) = retry {
        write_retry(&mut arguments, declared);
    }
    arguments
}

/// Writes the registration's retry declaration into a quorum queue's arguments, so the server
/// counts the deliveries and carries a spent one away on its own.
///
/// Only a declaration that names both halves is written: `RabbitMQ` dead-letters a spent delivery
/// only when the queue says where to, and a delivery limit on its own would drop it instead of
/// carrying it away. Subscribing refuses a half declaration rather than declaring one.
fn write_retry(arguments: &mut FieldTable, declared: &RetryDeclaration) {
    let (Some(max_attempts), Some(dead_letter)) = (declared.max_attempts(), declared.dead_letter())
    else {
        return;
    };
    // The server counts returns, not deliveries: it carries a message away once it has been
    // returned more often than the limit, so a limit of two is three deliveries.
    arguments.insert(
        ShortString::from(DELIVERY_LIMIT),
        AMQPValue::LongLongInt(i64::from(max_attempts.get()) - 1),
    );
    // A destination is a routing key in this crate's vocabulary, so a dead letter leaves through
    // the default exchange and lands in the queue of that name - unless the descriptor named an
    // exchange of its own, which stays the queue's word on where its rejections go.
    if !arguments
        .inner()
        .contains_key(&ShortString::from(DEAD_LETTER_EXCHANGE))
    {
        arguments.insert(
            ShortString::from(DEAD_LETTER_EXCHANGE),
            AMQPValue::LongString(String::new().into()),
        );
    }
    arguments.insert(
        ShortString::from(DEAD_LETTER_ROUTING_KEY),
        AMQPValue::LongString(dead_letter.to_owned().into()),
    );
}

/// Writes the settings both descriptors share onto `$queue`, whose only field is a [`QueueSpec`]
/// named `spec`.
///
/// The two descriptors differ in how a retry reaches the queue again, not in what a queue is, so
/// the settings that describe the queue itself are written once here and stay in step by
/// construction.
macro_rules! queue_settings {
    ($queue:ident) => {
        impl $queue {
            /// Binds the queue to `exchange` under `routing_key`.
            ///
            /// Call repeatedly for multiple bindings. Without any binding the queue only receives
            /// messages published to the default exchange under the queue name.
            #[must_use]
            pub fn bind(
                mut self,
                exchange: RabbitExchange,
                routing_key: impl Into<String>,
            ) -> Self {
                self.spec.bindings.push((exchange, routing_key.into()));
                self
            }

            /// Dead-letters rejected messages to `exchange` (the `x-dead-letter-exchange`
            /// argument).
            ///
            /// A handler returning drop settles with `basic.reject(requeue = false)`, which routes
            /// the message there. This is the queue's own topology, and it applies to every
            /// rejection, whoever caused it.
            #[must_use]
            pub fn dead_letter_exchange(mut self, exchange: impl Into<String>) -> Self {
                self.spec.set(
                    DEAD_LETTER_EXCHANGE,
                    AMQPValue::LongString(exchange.into().into()),
                );
                self
            }

            /// Overrides the routing key dead-lettered messages carry
            /// (`x-dead-letter-routing-key`).
            #[must_use]
            pub fn dead_letter_routing_key(mut self, routing_key: impl Into<String>) -> Self {
                self.spec.set(
                    DEAD_LETTER_ROUTING_KEY,
                    AMQPValue::LongString(routing_key.into().into()),
                );
                self
            }

            /// Sets one raw declaration argument (`x-...`), passed through verbatim.
            ///
            /// # Panics
            ///
            /// Panics if `name` exceeds 255 bytes (the AMQP short-string limit); argument names
            /// are compile-time constants in practice.
            #[must_use]
            pub fn argument(mut self, name: impl Into<String>, value: AMQPValue) -> Self {
                self.spec.set(name, value);
                self
            }

            /// Replaces the whole raw declaration argument table (`x-...` passthrough).
            #[must_use]
            pub fn arguments(mut self, arguments: FieldTable) -> Self {
                self.spec.arguments = arguments;
                self
            }

            /// Caps unacknowledged deliveries in flight for this subscription (`basic.qos`),
            /// overriding the broker-wide [`prefetch`](crate::LapinBroker::prefetch).
            ///
            /// This is the back-pressure window for the subscriber stream. When neither is set the
            /// server imposes no prefetch limit.
            ///
            /// The count is a [`NonZeroU16`] because AMQP reads `basic.qos(0)` as "no limit", the
            /// exact opposite of a cap: the zero sentinel is unrepresentable here, and leaving the
            /// prefetch unset is how "unlimited" is spelled.
            #[must_use]
            pub fn prefetch(mut self, prefetch: NonZeroU16) -> Self {
                self.spec.prefetch = Some(prefetch);
                self
            }

            /// Caps how long a partial batch waits for more deliveries before it goes to the
            /// handler. Defaults to 50 ms.
            ///
            /// AMQP delivers one message at a time, so a batch handler's `batch(n)` is honoured by
            /// collecting deliveries on the client; how big a batch may be is the registration's
            /// word, and this is the other half of the trade-off - the ceiling on how long a batch
            /// that never fills keeps its deliveries. Raise it on a slow link or a sparse queue
            /// where fuller batches are worth the wait; lower it where a batch arriving late costs
            /// more than a batch arriving short. It has no effect on a subscription without a
            /// batch handler.
            #[must_use]
            pub fn batch_wait(mut self, batch_wait: Duration) -> Self {
                self.spec.batch_wait = batch_wait;
                self
            }

            /// Makes `retry_after` / `nack_after` native, routing delayed redeliveries through a
            /// broker waiting queue instead of the core in-process fallback.
            ///
            /// Without this the runtime handles a delay with its broker-agnostic deferred
            /// re-publish (at-most-once over the delay window, held in the service). With it, a
            /// delayed message is re-published to the [`Delay`] waiting queue with a per-message
            /// TTL and dead-lettered back to this queue when it fires, so the delayed copy lives
            /// on the broker.
            ///
            /// The copy the waiting queue releases is a new message, which the server counts from
            /// zero; the framework's own retry count is what survives the round.
            ///
            /// The waiting queue is infrastructure: it is declared only when the broker opts into
            /// [`declare_topology`](crate::LapinBroker::declare_topology); otherwise provision it
            /// yourself.
            #[must_use]
            pub fn delay(mut self, delay: Delay) -> Self {
                self.spec.delay = Some(delay);
                self
            }

            /// The queue name.
            #[must_use]
            pub fn name(&self) -> &str {
                &self.spec.name
            }
        }
    };
}

pub(crate) use queue_settings;
