//! Durable delayed retry backed by a TTL waiting queue and a dead-letter exchange.
//!
//! `retry_after(delay)` (a handler returning
//! [`HandlerResult::retry_after`](ruststream::runtime::HandlerResult::retry_after), or a message
//! `nack_after`-ed) asks the broker to redeliver a message no sooner than `delay`. AMQP has no
//! native per-message delay, so without a delay queue the runtime falls back to core's
//! broker-agnostic deferred re-publish, which is at-most-once over the delay window and keeps the
//! delayed copy in the service process.
//!
//! A subscription that names a delay queue with [`RabbitQueue::delay`](crate::RabbitQueue::delay)
//! makes `nack_after` native (`supports_nack_after` reports `true`): the message is re-published
//! to a waiting queue with per-message `expiration = delay`, and the waiting queue's dead-letter
//! exchange routes it back to the origin queue once the TTL fires. The delayed copy lives on the
//! broker, not in the service.

use std::fmt;
use std::time::Duration;

use lapin::Channel;

/// How a subscription handles `retry_after` / `nack_after` delays.
///
/// Passed to [`RabbitQueue::delay`](crate::RabbitQueue::delay). There is no default that enables
/// it: a waiting queue is infrastructure the user owns, so opting in is explicit.
///
/// # Examples
///
/// ```
/// use ruststream_lapin::{Delay, RabbitQueue};
///
/// // Waiting queue named `orders.retry` (the default derived from the origin queue):
/// let orders = RabbitQueue::new("orders").delay(Delay::dlx_ttl());
/// // Or an explicit waiting-queue name:
/// let named = RabbitQueue::new("orders").delay(Delay::dlx_ttl_named("orders.wait"));
/// # let _ = (orders, named);
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
    /// quorum queue, which does not have this head-of-line constraint) when delays vary widely.
    DlxTtl {
        /// The waiting queue name; `None` derives `<origin>.retry`.
        waiting_queue: Option<String>,
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

    /// Resolves the waiting queue name for `origin`, applying the `<origin>.retry` default.
    pub(crate) fn waiting_queue_for(&self, origin: &str) -> String {
        match self {
            Self::DlxTtl {
                waiting_queue: Some(name),
            } => name.clone(),
            Self::DlxTtl {
                waiting_queue: None,
            } => format!("{origin}.retry"),
        }
    }
}

/// The resolved delay wiring a subscriber threads into each delivery, so a message can honor a
/// native `nack_after`. Cheap to clone (the channel is a handle).
#[derive(Clone)]
pub(crate) struct DelayContext {
    channel: Channel,
    waiting_queue: String,
}

impl DelayContext {
    pub(crate) fn new(channel: Channel, waiting_queue: String) -> Self {
        Self {
            channel,
            waiting_queue,
        }
    }

    pub(crate) fn channel(&self) -> &Channel {
        &self.channel
    }

    pub(crate) fn waiting_queue(&self) -> &str {
        &self.waiting_queue
    }

    /// Renders a `delay` as the AMQP per-message `expiration` (milliseconds, as a string).
    pub(crate) fn expiration(delay: Duration) -> String {
        u64::try_from(delay.as_millis())
            .unwrap_or(u64::MAX)
            .to_string()
    }
}

impl fmt::Debug for DelayContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DelayContext")
            .field("waiting_queue", &self.waiting_queue)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::{Delay, DelayContext};

    #[test]
    fn waiting_queue_name_defaults_to_origin_dot_retry() {
        assert_eq!(Delay::dlx_ttl().waiting_queue_for("orders"), "orders.retry");
        assert_eq!(
            Delay::dlx_ttl_named("orders.wait").waiting_queue_for("orders"),
            "orders.wait"
        );
    }

    #[test]
    fn expiration_renders_milliseconds() {
        assert_eq!(
            DelayContext::expiration(std::time::Duration::from_millis(1500)),
            "1500"
        );
        assert_eq!(
            DelayContext::expiration(std::time::Duration::from_secs(2)),
            "2000"
        );
    }
}
