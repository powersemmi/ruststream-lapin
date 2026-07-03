//! Per-delivery context fields exposed to handlers.
//!
//! [`AmqpContext`] carries the AMQP delivery metadata that is not part of the payload or the
//! headers. Request it in a handler by typing the context parameter as
//! `Context<'_, AmqpContext>` and read individual fields with the zero-sized keys in [`keys`].

use ruststream::{BuildContext, Field};

use crate::message::LapinMessage;

/// Native AMQP delivery metadata, built once per delivery.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AmqpContext {
    exchange: String,
    routing_key: String,
    redelivered: bool,
    delivery_tag: u64,
}

impl AmqpContext {
    /// The exchange the message was published to (empty for the default exchange).
    #[must_use]
    pub fn exchange(&self) -> &str {
        &self.exchange
    }

    /// The routing key the message was published with.
    #[must_use]
    pub fn routing_key(&self) -> &str {
        &self.routing_key
    }

    /// Whether the broker marked the delivery as redelivered.
    #[must_use]
    pub fn redelivered(&self) -> bool {
        self.redelivered
    }

    /// The channel-local delivery tag.
    #[must_use]
    pub fn delivery_tag(&self) -> u64 {
        self.delivery_tag
    }
}

impl BuildContext<LapinMessage> for AmqpContext {
    fn build(msg: &LapinMessage) -> Self {
        Self {
            exchange: msg.exchange().to_owned(),
            routing_key: msg.routing_key().to_owned(),
            redelivered: msg.redelivered(),
            delivery_tag: msg.delivery_tag(),
        }
    }
}

/// Zero-sized [`Field`] keys reading one [`AmqpContext`] field each.
pub mod keys {
    use super::{AmqpContext, Field};

    /// Reads the source exchange name.
    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
    pub struct Exchange;

    impl Field<AmqpContext> for Exchange {
        type Value<'a> = &'a str;

        fn get(self, src: &AmqpContext) -> &str {
            src.exchange()
        }
    }

    /// Reads the routing key.
    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
    pub struct RoutingKey;

    impl Field<AmqpContext> for RoutingKey {
        type Value<'a> = &'a str;

        fn get(self, src: &AmqpContext) -> &str {
            src.routing_key()
        }
    }

    /// Reads the redelivered flag.
    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
    pub struct Redelivered;

    impl Field<AmqpContext> for Redelivered {
        type Value<'a> = bool;

        fn get(self, src: &AmqpContext) -> bool {
            src.redelivered()
        }
    }

    /// Reads the channel-local delivery tag.
    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
    pub struct DeliveryTag;

    impl Field<AmqpContext> for DeliveryTag {
        type Value<'a> = u64;

        fn get(self, src: &AmqpContext) -> u64 {
            src.delivery_tag()
        }
    }
}
