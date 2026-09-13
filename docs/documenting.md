# The AsyncAPI document

A service generated from `#[ruststream::app]` prints its own AsyncAPI document with
`cargo run -- asyncapi gen`. The framework writes the channels, the messages and their schemas;
this crate fills the AMQP half of it, behind a feature:

```toml
ruststream-lapin = { version = "0.7", features = ["asyncapi"] }
```

Without the feature the document is generated all the same, with nothing RabbitMQ-specific in it.
With it, a service that consumes one queue and answers over direct reply-to reports this:

```json
--8<-- "crates/ruststream-lapin/tests/snapshots/asyncapi-amqp.json"
```

## The server

`host` and `protocol` say where clients connect and what they speak, and `protocolVersion` says
which AMQP that is. The distinction is not decoration: AMQP 1.0 is a different protocol with the
same name, a different binding key and different clients, and nothing in the host tells the two
apart.

The document is published and shared, so the credentials an AMQP URI carries never reach it. A
broker built from `amqp://svc:secret@rabbit:5672/prod` describes itself as `rabbit:5672`, password
and virtual host dropped.

## The channel a subscription consumes

A subscription's channel is a queue, and its address is the queue name. The `amqp` binding carries
what the descriptor says about that queue: its durability, whether it is exclusive to one
connection, and whether it is deleted with its last consumer.

The exchanges a queue is bound to have no place here. The specification's channel binding is
either a queue or a routing key, and this channel's address is the queue.

## The operation that receives

`ack: true` on the receive operation says the consumer acknowledges by hand. This crate always
does: a delivery is settled by the handler's outcome, never by the broker handing it out.

## The channel a publisher writes to

A publish policy describes the channel from the other side. `LapinPublish::default().exchange("x")`
is a routing key on the exchange `x`, so the binding reports `is: routingKey` and the exchange's
name. On the default exchange the routing key is a queue name, and the binding says so with
`is: queue`.

The send operation carries the properties every message through that policy takes: `deliveryMode`
(2 for the persistent default, 1 for `persistent(false)`), and `priority` and `expiration` where
the policy fixes them. A publish that adjusts a property for one message does not change the
document: the document describes the declaration, not a call.

A reply is the exception with no send operation of its own, so a reply policy's operation binding
reaches no document. Its channel binding does.

## Where a client reads a reply address

A handler mounted with `DirectReplyTo` answers each request wherever that request asked, so the
reply channel has no address to report. The operation names the header the address is read from
instead, `$message.header#/reply-to`, which is the direct reply-to convention written as a
runtime expression.

Without the transform the reply goes to the name the mount site declared, and that name is what
the channel reports.

## What stays empty

Two fields of the specification's AMQP binding have no honest source here. The virtual host lives
on the broker's connection URI, which no descriptor and no policy sees. The message type is the
Rust type's name, which the document already reports as the message's own name, and which a
binding hook never learns: it is handed the descriptor, not the message type.
