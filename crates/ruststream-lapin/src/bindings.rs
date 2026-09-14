//! What this crate adds to the generated `AsyncAPI` document: the AMQP 0.9.1 protocol bindings.
//!
//! A binding carries what only `RabbitMQ` knows about a channel, an operation or a message, and
//! the specification fixes both the key (`amqp`) and the shape of each object. Everything here is
//! computed from a descriptor or a publish policy alone, because the document is built before
//! anything connects, and none of it is a credential: the document is published and shared.
//!
//! Two fields of the specification's objects stay empty because nothing in this crate can fill
//! them honestly. The virtual host is on the broker's connection URI, which neither a descriptor
//! nor a policy sees; the message type is the Rust type's name, which the core already reports as
//! the message's own name and which a binding hook never learns, being handed the subscription or
//! the destination and nothing of what travels over it.

use ruststream::asyncapi::{Binding, Bindings};
use serde::Serialize;

use crate::broker::PROTOCOL;
use crate::publish_policy::PublishOptions;
use crate::queue::QueueSpec;

/// The version of the `amqp` binding objects this crate writes.
const BINDING_VERSION: &str = "0.3.0";

/// Delivery mode 2 marks a message persistent; 1 is transient.
const PERSISTENT: u8 = 2;
const TRANSIENT: u8 = 1;

/// Where a client reads the address of an answer: this crate routes a reply by the `reply-to`
/// header, as the direct reply-to convention does.
pub(crate) const REPLY_ADDRESS_LOCATION: &str = "$message.header#/reply-to";

/// A channel that is a queue: its address is the queue name, which is what a subscription binds
/// to and what a publish on the default exchange routes by.
#[derive(Serialize)]
struct QueueChannel<'a> {
    is: &'static str,
    queue: Queue<'a>,
}

#[derive(Serialize)]
struct Queue<'a> {
    name: &'a str,
    durable: bool,
    exclusive: bool,
    #[serde(rename = "autoDelete")]
    auto_delete: bool,
}

/// A channel that is a queue a publish addresses by name: the destination the mount site
/// resolved is the routing key, and on the default exchange a routing key is a queue name.
///
/// Only the name is written. A policy carries this crate's publish settings, and a queue's
/// durability is the declaring side's business, so the rest of the specification's queue object
/// has nobody here to fill it honestly.
#[derive(Serialize)]
struct PublishQueueChannel<'a> {
    is: &'static str,
    queue: PublishQueue<'a>,
}

#[derive(Serialize)]
struct PublishQueue<'a> {
    name: &'a str,
}

/// A channel that is a routing key: its address is the key a publish carries to the exchange.
#[derive(Serialize)]
struct RoutingKeyChannel<'a> {
    is: &'static str,
    exchange: Exchange<'a>,
}

#[derive(Serialize)]
struct Exchange<'a> {
    name: &'a str,
}

/// What a consumer of this crate does with a delivery.
#[derive(Serialize)]
struct ReceiveOperation {
    ack: bool,
}

/// The properties every message published through one policy carries.
#[derive(Serialize)]
struct SendOperation {
    #[serde(rename = "deliveryMode")]
    delivery_mode: u8,
    #[serde(skip_serializing_if = "Option::is_none")]
    priority: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    expiration: Option<u64>,
}

/// One `amqp` binding, or none at all: a description of itself never holds up a service.
fn one<T: Serialize>(body: &T) -> Bindings {
    Binding::new(PROTOCOL, BINDING_VERSION, body)
        .map(|binding| Bindings::new().with(binding))
        .unwrap_or_default()
}

/// The channel one queue subscription describes.
///
/// The exchanges the queue is bound to have no place here: the specification's channel object is
/// either a queue or a routing key, and this channel's address is the queue name.
pub(crate) fn queue_channel(spec: &QueueSpec) -> Bindings {
    one(&QueueChannel {
        is: "queue",
        queue: Queue {
            name: &spec.name,
            durable: spec.durable,
            exclusive: spec.exclusive,
            auto_delete: spec.auto_delete,
        },
    })
}

/// The receive operation one subscription describes: this crate always acknowledges by hand, so a
/// delivery is settled by the handler's outcome rather than by the broker handing it out.
pub(crate) fn consumer_operation() -> Bindings {
    one(&ReceiveOperation { ack: true })
}

/// The channel one publish policy describes: a routing key on the policy's exchange, or the queue
/// the key names when the policy publishes on the default exchange.
///
/// `destination` is what the mount site resolved for the position - a reply's name, a slot's own
/// name, a `dead_letter(..)` declaration - which on this crate is the routing key a publish
/// carries. On the default exchange that key is the queue it lands in, and naming it is all a
/// policy can add; on a named exchange the routing key is the channel's address and the exchange
/// is what the binding has room for.
pub(crate) fn publish_channel(options: &PublishOptions, destination: &str) -> Bindings {
    if options.exchange.is_empty() {
        one(&PublishQueueChannel {
            is: "queue",
            queue: PublishQueue { name: destination },
        })
    } else {
        one(&RoutingKeyChannel {
            is: "routingKey",
            exchange: Exchange {
                name: &options.exchange,
            },
        })
    }
}

/// The send operation one publish policy describes: the properties its messages carry unless a
/// publish adjusts them.
pub(crate) fn publish_operation(options: &PublishOptions) -> Bindings {
    let defaults = &options.defaults;
    one(&SendOperation {
        delivery_mode: if defaults.persistent.unwrap_or(true) {
            PERSISTENT
        } else {
            TRANSIENT
        },
        priority: defaults.priority,
        expiration: defaults
            .expiration
            .map(|ttl| u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX)),
    })
}
