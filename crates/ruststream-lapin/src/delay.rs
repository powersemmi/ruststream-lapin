//! Durable delayed retry, either through a TTL waiting queue and dead-letter exchange, or through
//! the delayed-message-exchange plugin.
//!
//! `retry_after(delay)` (a handler returning
//! [`HandlerOutcome::retry_after`](ruststream::runtime::HandlerOutcome::retry_after), or a message
//! `nack_after`-ed) asks the broker to redeliver a message no sooner than `delay`. AMQP has no
//! native per-message delay, so without a delay queue the runtime falls back to core's
//! broker-agnostic deferred re-publish, which is at-most-once over the delay window and keeps the
//! delayed copy in the service process.
//!
//! A subscription that opts in with [`RabbitQueue::delay`](crate::RabbitQueue::delay) makes
//! `nack_after` native (`supports_nack_after` reports `true`) and keeps the delayed copy on the
//! broker. Two backends are offered by [`Delay`]: the stock TTL waiting queue, and (behind the
//! `plugin-dme` feature) the delayed-message-exchange plugin.

use std::fmt;
use std::time::Duration;

use bytes::Bytes;
use lapin::Channel;
use lapin::options::{BasicPublishOptions, QueueDeclareOptions};
use lapin::types::{FieldTable, ShortString};
use ruststream::runtime::RETRY_COUNT_HEADER;
use ruststream::{HeaderMap, Str};
use thiserror::Error;

use crate::convert;
use crate::error::AmqpError;

/// How a subscription handles `retry_after` / `nack_after` delays.
///
/// Passed to [`RabbitQueue::delay`](crate::RabbitQueue::delay). There is no default that enables
/// it: a waiting queue or delayed exchange is infrastructure the user owns, so opting in is
/// explicit.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
///
/// use ruststream_lapin::prelude::*;
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Order {
///     id: u64,
///     paid: bool,
/// }
///
/// // An unpaid order waits a minute in `orders.retry` on the broker, then comes back here.
/// #[subscriber(RabbitQueue::new("orders").delay(Delay::dlx_ttl()))]
/// async fn fulfil(order: &Order) -> HandlerOutcome {
///     if !order.paid {
///         return HandlerOutcome::retry_after(Duration::from_mins(1));
///     }
///     println!("fulfilling order {}", order.id);
///     HandlerOutcome::ack()
/// }
///
/// fn app() -> RustStream {
///     let broker = LapinBroker::new("amqp://localhost:5672").declare_topology(true);
///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
///         b.include(fulfil);
///     })
/// }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Delay {
    /// Route delayed redeliveries through a TTL waiting queue whose dead-letter target is the
    /// origin queue.
    ///
    /// # Classic-queue caveat
    ///
    /// A classic queue releases expired messages only from its head, so a short-TTL message stuck
    /// behind a long-TTL one waits for the long one. Use one waiting queue per delay class (or a
    /// quorum queue, which does not have this head-of-line constraint) when delays vary widely, or
    /// switch to [`plugin_dme`](Self::plugin_dme), which has no head-of-line constraint.
    DlxTtl {
        /// The waiting queue name; `None` derives `<origin>.retry`.
        waiting_queue: Option<String>,
    },

    /// Route delayed redeliveries through the
    /// [`rabbitmq_delayed_message_exchange`](https://github.com/rabbitmq/rabbitmq-delayed-message-exchange)
    /// plugin: the message is re-published to an `x-delayed-message` exchange with an `x-delay`
    /// header, and the plugin releases it to the origin queue after the delay. Unlike the classic
    /// waiting queue, mixed delays do not block each other. Requires the plugin on the broker, so
    /// it is behind the `plugin-dme` feature.
    #[cfg(feature = "plugin-dme")]
    DelayedMessageExchange {
        /// The delayed-message exchange name; `None` derives `<origin>.delay`.
        exchange: Option<String>,
    },
}

impl Delay {
    /// A TTL waiting queue named `<origin>.retry`.
    #[must_use]
    pub const fn dlx_ttl() -> Self {
        Self::DlxTtl {
            waiting_queue: None,
        }
    }

    /// A TTL waiting queue with an explicit name.
    #[must_use]
    pub fn dlx_ttl_named(name: impl Into<String>) -> Self {
        Self::DlxTtl {
            waiting_queue: Some(name.into()),
        }
    }

    /// A delayed-message-exchange named `<origin>.delay`.
    #[cfg(feature = "plugin-dme")]
    #[must_use]
    pub const fn plugin_dme() -> Self {
        Self::DelayedMessageExchange { exchange: None }
    }

    /// A delayed-message-exchange with an explicit name (share one across queues if you like).
    #[cfg(feature = "plugin-dme")]
    #[must_use]
    pub fn plugin_dme_named(name: impl Into<String>) -> Self {
        Self::DelayedMessageExchange {
            exchange: Some(name.into()),
        }
    }

    /// Resolves the delay target for `origin`, applying the per-backend default names.
    pub(crate) fn target_for(&self, origin: &str) -> DelayTarget {
        match self {
            Self::DlxTtl {
                waiting_queue: Some(name),
            } => DelayTarget::WaitingQueue {
                waiting_queue: name.clone(),
            },
            Self::DlxTtl {
                waiting_queue: None,
            } => DelayTarget::WaitingQueue {
                waiting_queue: format!("{origin}.retry"),
            },
            #[cfg(feature = "plugin-dme")]
            Self::DelayedMessageExchange { exchange } => DelayTarget::DelayedExchange {
                exchange: exchange
                    .clone()
                    .unwrap_or_else(|| format!("{origin}.delay")),
                routing_key: origin.to_owned(),
            },
        }
    }
}

/// Where and how a delayed re-publish goes, resolved from the [`Delay`] backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DelayTarget {
    /// Publish to the default exchange with the waiting queue as the routing key and a per-message
    /// `expiration`; the waiting queue dead-letters back to the origin when the TTL fires.
    WaitingQueue { waiting_queue: String },
    /// Publish to the delayed-message exchange with the origin as the routing key and an `x-delay`
    /// header; the plugin releases the message to the origin after the delay.
    #[cfg(feature = "plugin-dme")]
    DelayedExchange {
        exchange: String,
        routing_key: String,
    },
}

impl DelayTarget {
    /// Checks, once, that the waiting queue or delayed exchange exists on the server, for a
    /// subscription whose broker does not declare it.
    ///
    /// The delayed copy is published without waiting for the server's answer and the original is
    /// acked after it. A copy sent to a waiting queue that does not exist is dropped by the server
    /// (the default exchange routes it nowhere), and one sent to a delayed exchange that does not
    /// exist closes the channel the subscription consumes on. Checking at startup turns both into
    /// a subscription that refuses to open.
    ///
    /// A missing target closes `channel` (a passive declaration answers "not found" that way), so
    /// it runs on the subscription's own channel before the consumer opens: the subscription fails
    /// with it either way.
    pub(crate) async fn ensure_exists(
        &self,
        channel: &Channel,
        subscription: &str,
    ) -> Result<(), AmqpError> {
        let missing = |what: &'static str, target: &str, source: lapin::Error| {
            AmqpError::Subscribe(Box::new(MissingDelayTarget {
                subscription: subscription.to_owned(),
                what,
                target: target.to_owned(),
                source,
            }))
        };
        match self {
            Self::WaitingQueue { waiting_queue } => {
                channel
                    .queue_declare(
                        convert::short(waiting_queue, "waiting queue name")?,
                        QueueDeclareOptions {
                            passive: true,
                            ..QueueDeclareOptions::default()
                        },
                        FieldTable::default(),
                    )
                    .await
                    .map_err(|err| missing("waiting queue", waiting_queue, err))?;
            }
            #[cfg(feature = "plugin-dme")]
            Self::DelayedExchange { exchange, .. } => {
                use lapin::ExchangeKind;
                use lapin::options::ExchangeDeclareOptions;

                channel
                    .exchange_declare(
                        convert::short(exchange, "delayed exchange name")?,
                        ExchangeKind::Custom("x-delayed-message".to_owned()),
                        ExchangeDeclareOptions {
                            passive: true,
                            ..ExchangeDeclareOptions::default()
                        },
                        FieldTable::default(),
                    )
                    .await
                    .map_err(|err| missing("delayed exchange", exchange, err))?;
            }
        }
        Ok(())
    }
}

/// A delay target the subscription's broker does not declare, and the server does not have.
#[derive(Debug, Error)]
#[error(
    "subscription {subscription:?} delays its retries through the {what} {target:?}, which the \
     server does not have; provision it, or let the broker declare it with \
     declare_topology(true)"
)]
struct MissingDelayTarget {
    subscription: String,
    what: &'static str,
    target: String,
    #[source]
    source: lapin::Error,
}

/// The resolved delay wiring a subscriber threads into each delivery, so a message can honor a
/// native `nack_after`. Cheap to clone (the channel is a handle).
#[derive(Clone)]
pub(crate) struct DelayContext {
    channel: Channel,
    target: DelayTarget,
}

impl DelayContext {
    pub(crate) fn new(channel: Channel, target: DelayTarget) -> Self {
        Self { channel, target }
    }

    /// Re-publishes `payload` so the broker redelivers it to the origin queue after `delay`.
    ///
    /// Sent on the delivery's own channel, so it orders before the original ack.
    ///
    /// The copy carries the framework's retry count, raised by one. The waiting queue releases a
    /// new message, and the server counts a new message from zero, so this header is the only
    /// record that the delivery has been round before - the same record the runtime keeps on the
    /// copies it publishes itself.
    pub(crate) async fn republish(
        &self,
        payload: &[u8],
        headers: &HeaderMap,
        delay: Duration,
    ) -> Result<(), AmqpError> {
        // A redelivery has no call site to adjust anything, so the copy carries what the original
        // delivery reported.
        let options = convert::redelivery_options(headers);
        let headers = &counted_again(headers);
        match &self.target {
            DelayTarget::WaitingQueue { waiting_queue } => {
                // The waiting queue's own TTL replaces whatever expiration the delivery carried:
                // the delay is what the copy waits for there.
                let properties = convert::properties_for_publish(headers, &options)?
                    .with_expiration(convert::expiration_millis(delay));
                self.channel
                    .basic_publish(
                        ShortString::default(),
                        convert::short(waiting_queue, "waiting queue name")?,
                        BasicPublishOptions::default(),
                        payload,
                        properties,
                    )
                    .await
                    .map_err(AmqpError::publish)?;
            }
            #[cfg(feature = "plugin-dme")]
            DelayTarget::DelayedExchange {
                exchange,
                routing_key,
            } => {
                use lapin::types::{AMQPValue, FieldTable};

                let mut properties = convert::properties_for_publish(headers, &options)?;
                let mut table = properties
                    .headers()
                    .clone()
                    .unwrap_or_else(FieldTable::default);
                // The plugin reads `x-delay` (milliseconds) and holds the message for that long.
                let millis = i64::try_from(delay.as_millis()).unwrap_or(i64::MAX);
                table.insert(ShortString::from("x-delay"), AMQPValue::LongLongInt(millis));
                properties = properties.with_headers(table);
                self.channel
                    .basic_publish(
                        convert::short(exchange, "delayed exchange name")?,
                        convert::short(routing_key, "routing key")?,
                        BasicPublishOptions::default(),
                        payload,
                        properties,
                    )
                    .await
                    .map_err(AmqpError::publish)?;
            }
        }
        Ok(())
    }
}

/// The delivery's headers with the framework's retry count raised by one; an absent count is zero,
/// so the first copy leaves with one.
pub(crate) fn counted_again(headers: &HeaderMap) -> HeaderMap {
    let spent = headers
        .get_str(RETRY_COUNT_HEADER)
        .and_then(|count| count.parse::<u64>().ok())
        .unwrap_or(0);
    let mut headers = headers.clone();
    headers.insert(
        Str::from_static(RETRY_COUNT_HEADER),
        Bytes::from(spent.saturating_add(1).to_string()),
    );
    headers
}

impl fmt::Debug for DelayContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DelayContext")
            .field("target", &self.target)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::{Delay, DelayTarget};

    #[test]
    fn dlx_ttl_target_defaults_to_origin_dot_retry() {
        assert_eq!(
            Delay::dlx_ttl().target_for("orders"),
            DelayTarget::WaitingQueue {
                waiting_queue: "orders.retry".to_owned()
            }
        );
        assert_eq!(
            Delay::dlx_ttl_named("orders.wait").target_for("orders"),
            DelayTarget::WaitingQueue {
                waiting_queue: "orders.wait".to_owned()
            }
        );
    }

    #[cfg(feature = "plugin-dme")]
    #[test]
    fn dme_target_defaults_to_origin_dot_delay() {
        assert_eq!(
            Delay::plugin_dme().target_for("orders"),
            DelayTarget::DelayedExchange {
                exchange: "orders.delay".to_owned(),
                routing_key: "orders".to_owned(),
            }
        );
    }
}
