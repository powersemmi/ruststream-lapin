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

use lapin::ExchangeKind;
use lapin::types::{AMQPValue, FieldTable};
use ruststream::asyncapi::{Binding, Bindings};
use serde::Serialize;
use serde::ser::{SerializeMap, SerializeSeq, Serializer};

use crate::broker::PROTOCOL;
use crate::publish_policy::PublishOptions;
use crate::queue::QueueSpec;

/// The version of the `amqp` binding objects this crate writes.
const BINDING_VERSION: &str = "0.3.0";

/// Where this crate reports what the `amqp` binding has no field for.
///
/// The specification's AMQP 0-9-1 binding describes a channel as a queue or as a routing key and
/// says nothing about what binds the two: no exchange, no binding key, no arguments. The request
/// for them (`asyncapi/bindings#263`) was closed as not planned, so a reader who needs the
/// routing table this service expects finds it here instead. An extension carries no
/// `bindingVersion`: the field belongs to a binding, and this is not one.
pub(crate) const BINDINGS_EXTENSION: &str = "x-ruststream-amqp";

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

/// The bindings one queue descriptor declares, in the order it declared them.
#[derive(Serialize)]
struct DeclaredBindings<'a> {
    bindings: Vec<DeclaredBinding<'a>>,
}

/// One binding: what the queue is bound to, under which key, and what the exchange matches on.
///
/// The routing key is written even when it is empty, because an empty key is a binding key like
/// any other - it is what a fanout or a headers exchange is bound under. The arguments are left
/// out when there are none, which is every exchange that routes on the key alone.
#[derive(Serialize)]
struct DeclaredBinding<'a> {
    exchange: &'a str,
    #[serde(rename = "type")]
    kind: &'a str,
    #[serde(rename = "routingKey")]
    routing_key: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    arguments: Option<Arguments<'a>>,
}

/// An argument table as JSON: the names as they are, the values in the shape their AMQP type
/// maps onto.
struct Arguments<'a>(&'a FieldTable);

impl Serialize for Arguments<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let table = self.0.inner();
        let mut map = serializer.serialize_map(Some(table.len()))?;
        for (name, value) in table {
            map.serialize_entry(name.as_str(), &Argument(value))?;
        }
        map.end()
    }
}

/// One argument value.
struct Argument<'a>(&'a AMQPValue);

impl Serialize for Argument<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self.0 {
            AMQPValue::Boolean(value) => serializer.serialize_bool(*value),
            AMQPValue::ShortShortInt(value) => serializer.serialize_i8(*value),
            AMQPValue::ShortShortUInt(value) => serializer.serialize_u8(*value),
            AMQPValue::ShortInt(value) => serializer.serialize_i16(*value),
            AMQPValue::ShortUInt(value) => serializer.serialize_u16(*value),
            AMQPValue::LongInt(value) => serializer.serialize_i32(*value),
            AMQPValue::LongUInt(value) => serializer.serialize_u32(*value),
            AMQPValue::LongLongInt(value) => serializer.serialize_i64(*value),
            AMQPValue::Float(value) => serializer.serialize_f32(*value),
            AMQPValue::Double(value) => serializer.serialize_f64(*value),
            AMQPValue::Timestamp(value) => serializer.serialize_u64(*value),
            AMQPValue::ShortString(value) => serializer.serialize_str(value.as_str()),
            // A long string carries bytes, and an argument that is not text is not a name the
            // document can report; the lossy reading is what a reader can act on.
            AMQPValue::LongString(value) => {
                serializer.serialize_str(&String::from_utf8_lossy(value.as_bytes()))
            }
            AMQPValue::ByteArray(value) => {
                let bytes = value.as_slice();
                let mut list = serializer.serialize_seq(Some(bytes.len()))?;
                for byte in bytes {
                    list.serialize_element(byte)?;
                }
                list.end()
            }
            AMQPValue::FieldArray(values) => {
                let values = values.as_slice();
                let mut list = serializer.serialize_seq(Some(values.len()))?;
                for value in values {
                    list.serialize_element(&Argument(value))?;
                }
                list.end()
            }
            AMQPValue::FieldTable(table) => Arguments(table).serialize(serializer),
            AMQPValue::DecimalValue(value) => {
                let mut decimal = serializer.serialize_map(Some(2))?;
                decimal.serialize_entry("scale", &value.scale)?;
                decimal.serialize_entry("value", &value.value)?;
                decimal.end()
            }
            AMQPValue::Void => serializer.serialize_none(),
        }
    }
}

/// The exchange type a binding points at, as the server names it.
fn exchange_type(kind: &ExchangeKind) -> &str {
    match kind {
        ExchangeKind::Custom(kind) => kind.as_str(),
        ExchangeKind::Direct => "direct",
        ExchangeKind::Fanout => "fanout",
        ExchangeKind::Headers => "headers",
        ExchangeKind::Topic => "topic",
    }
}

/// One `amqp` binding, or none at all: a description of itself never holds up a service.
fn one<T: Serialize>(body: &T) -> Bindings {
    Binding::new(PROTOCOL, BINDING_VERSION, body)
        .map(|binding| Bindings::new().with(binding))
        .unwrap_or_default()
}

/// The channel one queue subscription describes: the queue in the `amqp` binding, and what binds
/// it to its exchanges in [`BINDINGS_EXTENSION`] beside it.
///
/// The specification's channel object is either a queue or a routing key, so the bindings have no
/// field of their own there; a subscription that declares none reports no extension at all.
pub(crate) fn queue_channel(spec: &QueueSpec) -> Bindings {
    let mut reported = one(&QueueChannel {
        is: "queue",
        queue: Queue {
            name: &spec.name,
            durable: spec.durable,
            exclusive: spec.exclusive,
            auto_delete: spec.auto_delete,
        },
    });
    if spec.bindings.is_empty() {
        return reported;
    }

    let declared = DeclaredBindings {
        bindings: spec
            .bindings
            .iter()
            .map(|binding| DeclaredBinding {
                exchange: binding.exchange.name(),
                kind: exchange_type(binding.exchange.kind()),
                routing_key: &binding.routing_key,
                arguments: (!binding.arguments.inner().is_empty())
                    .then_some(Arguments(&binding.arguments)),
            })
            .collect(),
    };
    if let Ok(extension) = Binding::extension(BINDINGS_EXTENSION, &declared) {
        reported = reported.with(extension);
    }
    reported
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
