`RabbitMQ` over AMQP 0.9.1 for [RustStream](https://github.com/powersemmi/ruststream)
services, backed by the [`lapin`] client.

A queue here is destructive storage: a delivery leaves the queue once it is acknowledged. That
acknowledgement, a requeue and a dead-letter route are all protocol frames, so the framework
settles a message on the server instead of keeping a copy in the service. The crate covers the
AMQP 0.9.1 topology a service meets - classic and quorum queues, the stock exchange types and
two plugin ones, bindings, publisher confirms, channel transactions and direct reply-to - and
ships an in-process transport that runs the same handlers without a server.

```toml
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-lapin = "0.7"
serde = { version = "1", features = ["derive"] }
```

# The first service

[`LapinBroker::new`] performs no I/O, so a `RabbitMQ` service is assembled by the synchronous
`#[ruststream::app]` builder like any other. The runtime connects the broker once at startup and
opens the subscriptions off the connected form.

```
# mod demo {
use ruststream_lapin::prelude::*;
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u64,
}

#[subscriber("orders")]
async fn handle(order: &Order) -> HandlerOutcome {
    println!("got order {}", order.id);
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        LapinBroker::new("amqp://localhost:5672").prefetch(nonzero!(64)),
        |b| {
            b.include(handle);
        },
    )
}
# }
# fn main() {}
```

`cargo run -- run` starts it, and `cargo run -- asyncapi gen` prints the service's own document.
The queue `orders` has to exist: this crate creates no infrastructure until a broker opts in (see
[Operations](#operations)).

A handler body names capabilities - `Out<impl Publisher>`, `Out<impl TransactionalPublisher>`,
`Out<impl RequestReply>` - and imports the framework's prelude alone, so one handler mounts on a
server and on the in-process transport unchanged. A routes file names values and imports
[`prelude`], which carries the framework's prelude with it.

Two runnable services live in `examples/`: `lapin_quickstart` is the code above, `lapin_topology`
adds descriptors and declaration.

# Subscribing

One subscription consumes one queue. `RabbitMQ` has two queue implementations, the implementation
is fixed when the queue is created, and it decides what a retry does - so it is a descriptor type
rather than a setting:

| Descriptor | Queue | A retry is |
|---|---|---|
| [`RabbitQueue`] | classic, single-node | counted by the runtime, on copies it publishes back |
| [`RabbitQuorumQueue`] | quorum, Raft-replicated | counted by the server, which moves a spent one |

`#[subscriber("orders")]` consumes the classic queue named `orders` with the descriptor defaults.
Neither descriptor has a bare-type form: a queue that is not the plain default is written out in
full in the attribute, and every step returns `Self`.

```
# mod demo {
use std::time::Duration;

use ruststream_lapin::prelude::*;
use serde::Deserialize;

#[derive(Deserialize)]
struct OrderPlaced {
    id: u64,
}

// One queue fed by a topic exchange, with a bounded window of unacknowledged deliveries.
#[subscriber(RabbitQueue::new("orders")
    .bind(RabbitExchange::topic("events"), "order.*")
    .dead_letter_exchange("dead-letters")
    .prefetch(nonzero!(16)))]
async fn on_order(event: &OrderPlaced) -> HandlerOutcome {
    println!("order event {}", event.id);
    HandlerOutcome::ack()
}

// A slice parameter asks for a batch; the mount site says how big it may get.
#[subscriber(RabbitQueue::new("settlements")
    .prefetch(nonzero!(64))
    .batch_wait(Duration::from_millis(200)))]
async fn on_settlement(events: &[OrderPlaced]) -> HandlerOutcome {
    println!("settling {} orders", events.len());
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    let broker = LapinBroker::new("amqp://localhost:5672").declare_topology(true);
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(on_order);
        b.include(on_settlement.batch(nonzero!(32)));
    })
}
# }
# fn main() {}
```

Both descriptors describe the same queue: [`bind`](RabbitQueue::bind) for exchange bindings,
[`bind_with`](RabbitQueue::bind_with) where the exchange routes by the binding's arguments,
[`dead_letter_exchange`](RabbitQueue::dead_letter_exchange) and
[`dead_letter_routing_key`](RabbitQueue::dead_letter_routing_key) for the queue's own rejection
route, [`prefetch`](RabbitQueue::prefetch), [`batch_wait`](RabbitQueue::batch_wait),
[`delay`](RabbitQueue::delay), and [`argument`](RabbitQueue::argument) /
[`arguments`](RabbitQueue::arguments) for anything the descriptor does not model
([`AMQPValue`] and [`FieldTable`] are re-exported for that). A quorum queue is durable, shared and
permanent by definition, so `durable`, `exclusive` and `auto_delete` are not on it: a queue that
contradicts its own type does not compile.

[`RabbitExchange`] names the exchange a binding points at: `direct` routes on an exact routing
key, `topic` on a dotted pattern, `fanout` on nothing at all, and `headers` on the binding's
arguments rather than the key. The last one is what `bind_with` exists for - `x-match` decides
whether a message has to carry `all` of the remaining entries or any one of them, and the routing
key is ignored:

```rust
use ruststream_lapin::{AMQPValue, FieldTable, RabbitExchange, RabbitQueue};

let mut gold_in_eu = FieldTable::default();
gold_in_eu.insert("x-match".into(), AMQPValue::LongString("all".into()));
gold_in_eu.insert("region".into(), AMQPValue::LongString("eu".into()));
gold_in_eu.insert("tier".into(), AMQPValue::LongString("gold".into()));

let both = RabbitQueue::new("orders.eu.gold")
    .bind_with(RabbitExchange::headers("orders"), "", gold_in_eu);

let mut either = FieldTable::default();
either.insert("x-match".into(), AMQPValue::LongString("any".into()));
either.insert("region".into(), AMQPValue::LongString("eu".into()));
either.insert("tier".into(), AMQPValue::LongString("gold".into()));

// One header is enough here, where the binding above needs both.
let some = RabbitQueue::new("orders.eu.or.gold")
    .bind_with(RabbitExchange::headers("orders"), "", either);
# let _ = (both, some);
```

Binding to a headers exchange with the plain `bind` takes the whole exchange instead of a slice of
it, because `all` of no headers is true of every message, so `bind` is for the exchanges that route
on the key.

## Settling a delivery

Settlement is native, with no client-side republishing: ack sends `basic.ack`, retry sends
`basic.reject` with requeue, drop sends `basic.reject` without it, which dead-letters the message
where the queue has a dead-letter exchange. `RabbitMQ` counts a rejection as a delivery the message
has spent and counts `basic.nack` not at all, which is why the crate never sends one - and why a
handler that keeps asking for its message back runs a quorum queue's own limit down. A quorum
delivery reports how far it has come through `redelivery_count()`; a classic one reports no count,
only the `redelivered` flag that the message has been seen before.

## Capping the retries

A handler that keeps asking for another delivery circulates its message until an operator steps in.
`max_attempts(n)` is how many deliveries one message gets, counting the first, and
`dead_letter(name)` is where it goes once they run out; a destination is a routing key here, so the
name is the queue a spent delivery lands in.

On a quorum queue the pair becomes the queue's own `x-delivery-limit` and dead-letter route, which
the server applies whether or not a service is running. It reaches the queue only where this
service declares it, so the broker has to opt into
[`declare_topology(true)`](LapinBroker::declare_topology); a queue declared elsewhere carries
whatever arguments its owner gave it, and subscribing refuses the pair there rather than promising
a cap nothing applies. Half a declaration is refused on either route, because a limit with nowhere
to send the spent delivery drops it. Nothing of this process is published on that path, which is
what makes `out_retry(policy)` a compile error on a quorum queue.

A classic queue counts nothing, so the cap is the runtime's: it counts the attempts in the
`x-ruststream-retry-count` header of the copies it publishes, and a copy goes back under the
queue's own name, which is what addresses the queue on the default exchange. The copies leave
through the broker's default publish policy unless `out_retry(policy)` names another, and that
position takes the slot's own steps (`.codec(..)`, `.transform(..)`, `.to(name)`).

A bare queue name takes the runtime's cap too. `#[subscriber("orders")]` carries no arguments to
declare a queue with, so a name arriving here is a queue that already exists and the two steps
stay what the runtime applies, whatever that queue turns out to be. A quorum queue that is to
carry a spent delivery away itself gets `x-delivery-limit` and a dead-letter route where it is
declared: from [`RabbitQuorumQueue`] in a service that owns the queue, on the server in a service
that does not.

```
# mod demo {
use ruststream_lapin::prelude::*;
use serde::Deserialize;

# #[derive(Deserialize)]
# struct Payout {
#     id: u64,
# }
#[subscriber(RabbitQuorumQueue::new("payouts"))]
async fn on_payout(payout: &Payout) -> HandlerOutcome {
    HandlerOutcome::retry()
}

#[subscriber(RabbitQueue::new("refunds"))]
async fn on_refund(refund: &Payout) -> HandlerOutcome {
    HandlerOutcome::retry()
}

#[ruststream::app]
fn app() -> impl App {
    let broker = LapinBroker::new("amqp://localhost:5672").declare_topology(true);
    RustStream::new(AppInfo::new("payouts", "0.1.0")).with_broker(broker, |b| {
        // The server applies this one: it becomes the queue's delivery limit and dead-letter
        // route.
        b.include(on_payout)
            .max_attempts(nonzero!(5u32))
            .dead_letter("payouts.dead");
        // The runtime applies this one, and the confirms publisher owns the copy it sends.
        b.include(on_refund)
            .max_attempts(nonzero!(5u32))
            .dead_letter("refunds.dead")
            .out_retry(TransactionalPublish::default());
    })
}
# }
# fn main() {}
```

Where a copy goes is the descriptor's answer, and the two differ: [`RabbitQueue`] answers
`AddressedCopies` with the queue name, [`RabbitQuorumQueue`] answers `BrokerMoves` and publishes
nothing. A retry publisher aimed at a topic or direct exchange reaches the queue only through a
binding under that name, so bind it there or leave the retry publisher on the default exchange.

## Delayed redelivery

`HandlerOutcome::retry_after(delay)` asks for a redelivery no sooner than `delay`, the
not-ready-yet case an immediate requeue would spin on. AMQP has no per-message delay, so by default
the runtime holds the copy in the service process and republishes it when the window is up:
at-most-once over that window, and a restart mid-window loses it.

[`delay`](RabbitQueue::delay) puts the wait on the broker instead. With [`Delay::dlx_ttl`] the
message parks in a waiting queue (`<queue>.retry` by default) under a per-message TTL and
dead-letters back to the origin when it fires; the `plugin-dme` feature offers
[`Delay::plugin_dme`], which hands the message to the delayed-message-exchange plugin instead, so
a short delay never waits behind a long one. A classic waiting queue releases expired messages
only from its head, so mixed delays want one waiting queue per delay class, a quorum queue, or the
plugin. Either waiting queue is infrastructure, declared only under `declare_topology(true)`.

The copy the wait releases is a new message, which the server counts from zero, so it carries the
framework's `x-ruststream-retry-count` raised by one. The runtime reads that count on this path as
well, so a cap declared over a queue with [`delay`](RabbitQueue::delay) ends the loop the way it
ends an immediate one: the spent delivery goes to the dead-letter queue instead of waiting once
more.

## Batches

AMQP pushes one `basic.deliver` at a time, so a `&[T]` handler's batch is assembled on the client:
deliveries collect until the batch holds the size the mount site asked for with `.batch(n)` or
[`batch_wait`](RabbitQueue::batch_wait) elapses since the first of them. The wait defaults to 50
ms. A batch may be shorter than the size, and it holds only what the broker has already pushed, so
pair `.batch(n)` with a prefetch of at least `n`.

## Delivery metadata

[`AmqpContext`](context::AmqpContext) carries what is neither payload nor headers: the exchange,
the routing key, the redelivered flag and the channel-local delivery tag. The prelude carries the
four keys - [`Exchange`](context::keys::Exchange), [`RoutingKey`](context::keys::RoutingKey),
[`Redelivered`](context::keys::Redelivered), [`DeliveryTag`](context::keys::DeliveryTag) - and a
handler names the fields it needs one key at a time, or types its context parameter
`Context<'_, AmqpContext>` and reads them with `ctx.context(KEY)`.

```
# mod demo {
use ruststream_lapin::prelude::*;
use serde::Deserialize;

# #[derive(Deserialize)]
# struct Order {
#     id: u64,
# }
#[subscriber(RabbitQueue::new("orders-audit"))]
async fn audit(
    order: &Order,
    Ctx(routing_key): Ctx<RoutingKey>,
    Ctx(redelivered): Ctx<Redelivered>,
) -> HandlerOutcome {
    println!("order {} via {routing_key} (redelivered: {redelivered})", order.id);
    HandlerOutcome::ack()
}
# }
# fn main() {}
```

`workers(n, by_key)` spreads one queue over `n` lanes while keeping deliveries that share a
partition key on one lane, ordered per key. The producer sets the key in
[`PARTITION_KEY_HEADER`] (`amqp-partition-key`); AMQP does not interpret it, so this is a
client-side convention. Server-side fan-out across several queues is the separate
`plugin-consistent-hash` feature and its
`RabbitExchange::consistent_hash` constructor, where each queue binds with its weight as the
routing key.

A queue keeps no history to reposition into, so the crate implements neither `Seekable` nor
`Positioned` and there is no start position to name.

# Publishing

[`OutgoingMessage::name`](ruststream::OutgoingMessage) is the routing key, and the exchange belongs
to the publish policy: the default exchange unless [`exchange`](LapinPublish::exchange) names
another. On the default exchange a routing key addresses the queue of that name, which is why the
first service above needs no topology at all.

A policy holds no connection, so it is constructible anywhere; the runtime pairs it with the
connected broker at startup and the handler receives the live publisher. Under the family's uniform
names, which [`prelude`] aliases:

| Mount site | Policy | What a publish through it means |
|---|---|---|
| `Publish` | [`LapinPublish`] | fire-and-forget: it resolves when the frame is written |
| `TransactionalPublish` | [`ConfirmsPublish`] | confirms: it resolves when the broker confirmed |
| `ServerTxPublish` | [`ServerTxPublish`] | channel transactions: visible at commit |
| `Request` | [`LapinRequest`] | a request over direct reply-to, resolved by the reply |

The publishing mode is a policy transition rather than a flag: [`confirms`](LapinPublish::confirms)
and [`server_tx`](LapinPublish::server_tx) move to the stronger policies, keeping the settings, so
a handler bounded `Out<impl TransactionalPublisher>` mounts against those names and against nothing
weaker. [`ServerTxPublish`] keeps its own name because atomic visibility, bought with a synchronous
commit round trip, is a different guarantee and not a second spelling of confirms.

## Per-message properties

Three AMQP properties belong to the message rather than to the publisher: the `priority`, the
per-message `expiration` (TTL) and the delivery mode. The policy fixes what every message through
it carries ([`priority`](LapinPublish::priority), [`expiration`](LapinPublish::expiration),
[`persistent`](LapinPublish::persistent), which defaults to on), and the publish builder's steps
adjust one message over that.

```
# mod demo {
use std::time::Duration;

use ruststream_lapin::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct Order {
    id: u64,
    expedited: bool,
}

#[derive(Serialize, Outgoing)]
#[outgoing(name = "shipments")]
struct Shipment {
    order_id: u64,
}

#[subscriber("orders")]
async fn ship(
    order: &Order,
    Out(shipments): Out<impl Publisher<Options = LapinPublishOptions>>,
) -> HandlerOutcome {
    let shipment = Shipment { order_id: order.id };
    let sent = if order.expedited {
        shipments
            .message(&shipment)
            .priority(9)
            .expiration(Duration::from_secs(3600))
            .publish()
            .await
    } else {
        shipments.message(&shipment).publish().await
    };
    if sent.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    let broker = LapinBroker::new("amqp://localhost:5672");
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(ship)
            .out(DefaultSlot, Publish::default().priority(3))
            .build();
    })
}
# }
# fn main() {}
```

A step is a position on the publish builder, never a wrapper around the publisher, so a stepped
publish keeps the codec, the destination and the transforms the mount site gave it, holds through
both transaction kinds, and stays attributed to its `Out` slot under the test harness. This is the
one handler body that imports this crate's prelude: the slot is bounded by the options type
([`LapinPublishOptions`]) rather than by a publisher type, so the signature still names no broker.

None of the three travels as a header. Written into the header table under the protocol's own names
they would reach `RabbitMQ` as table entries it reads for no purpose, and the message would arrive
without the property. A delivery does report them back as headers, under [`PRIORITY_HEADER`] and
[`EXPIRATION_HEADER`], so a handler reads an incoming priority where it reads everything else.

Of the headers a message carries, four ride in the matching native property - `content-type`,
`correlation-id`, `reply-to` and `message-id` - so an external consumer finds them where the
protocol puts them. Every other header lands in the AMQP header table as a byte string, and binary
values round-trip.

## Replies

A publishing handler returns its reply and the runtime publishes it through the policy named at
`.out_reply(..)`. Where it goes is the reply type's word: `#[outgoing(name = "..")]` fixes the
routing key, and a type that declares none takes the name the `publish("..")` clause gives. The
steps after the position fill the rest of the wiring, `.codec(..)` for the reply codec and
`.transform(..)` for a change to each reply before it leaves. The whole reply surface is the
core's: <https://docs.rs/ruststream/latest/ruststream/runtime/index.html#replies>.

## Transactions

Both transactional publishers implement the framework's `TransactionalPublisher`, so
`begin_transaction` / `commit` / `abort` reads the same on either. A call that is invalid in the
current state returns an error rather than passing silently: a commit with nothing open, or a
second begin while one is open, which leaves the open transaction intact. A publisher is a
reference-counted handle, so every clone shares one channel and one transaction state.

[`ConfirmsPublish`] also offers the owned kind: `OwnedTransactions::transaction` hands back a
[`ConfirmsTransaction`] that owns its buffer, so any number can be open at once, settling one never
touches another, and the handle keeps publishing directly meanwhile. `commit` and `abort` consume
it, so a double commit does not compile. [`ServerTxPublish`] offers only the borrowed kind, because
`tx.select` puts the channel itself into transactional mode and a channel has exactly one such
state.

Reach for the owned kind when one handler drives several independent groups of messages, and the
borrowed one when a whole scope publishes into one shared transaction. A failed commit consumes the
owned transaction and loses its buffer: redelivery of the input, not resubmission of the buffer, is
the recovery path.

## Request and reply

[`LapinRequest`] pairs into [`LapinRequester`], which implements `RequestReply` over `RabbitMQ`
[direct reply-to](https://www.rabbitmq.com/docs/direct-reply-to). Every request goes out with
`reply-to` set to the `amq.rabbitmq.reply-to` pseudo-queue and a generated `correlation-id`, and
the correlated reply resolves the call. The replies are read by one task per requester, started
by its first request on the runtime the broker connected on, so a handler on a dedicated thread may
make that request from a runtime that stops afterwards. Requests are transient by default - a request nobody is
waiting for after the timeout gains nothing from surviving a restart - and
[`persistent(true)`](LapinRequest::persistent) opts back in.

The responder is an ordinary publishing handler; what makes it an RPC responder is
[`DirectReplyTo`] composed onto the reply publisher, which sends each reply to the address the
request asked for and echoes its correlation id.

```
# mod demo {
use ruststream_lapin::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct CheckStock {
    sku: String,
}

// An RPC reply goes wherever the request asked, so the type declares no destination.
#[derive(Serialize, Outgoing)]
struct Stock {
    available: bool,
}

#[subscriber("inventory.check", publish("inventory.check.unrouted"))]
async fn check(req: &CheckStock) -> Stock {
    Stock {
        available: !req.sku.is_empty(),
    }
}

#[ruststream::app]
fn app() -> impl App {
    let broker = LapinBroker::new("amqp://localhost:5672").declare_topology(true);
    RustStream::new(AppInfo::new("inventory", "0.1.0")).with_broker(broker, |b| {
        b.include(check)
            .out_reply(Publish::default())
            .transform(DirectReplyTo);
    })
}
# }
# fn main() {}
```

The transform names the destination per delivery, so it mounts only where nothing has declared one:
a reply type with its own `#[outgoing(name = "..")]` refuses it at the mount site. The
`publish("..")` name is the fallback a request without a `reply-to` header is answered at, and it
is what the generated document reports.

Direct reply-to is at-most-once: the reply state lives in the requester's channel on one broker
node, nothing is queued durably, and a dropped channel loses the replies in flight, which the
caller sees as a timeout. Nothing is declared for it either, so the only real entity is the request
queue the responder consumes; a responder on another stack interoperates by publishing to the
received `reply-to` and echoing the `correlation-id`. The runnable pair is `lapin_rpc_server` and
`lapin_rpc_client` in `examples/`.

# The prelude

[`prelude`] is the one glob a routes file writes. It re-exports the framework's own prelude, this
crate's broker and descriptors ([`LapinBroker`], [`RabbitQueue`], [`RabbitQuorumQueue`],
[`RabbitExchange`], [`Delay`]), the publish policies under both their own names and the family's
uniform ones, [`DirectReplyTo`], the delivery-context keys, the raw-argument types
([`AMQPValue`], [`FieldTable`]), and the transaction traits a settled value needs.

A handler body imports the framework's prelude alone. The one exception is a body that adjusts an
AMQP property for a single message: [`LapinPublishSteps`] comes from here, and the slot it runs on
is bounded `Out<impl Publisher<Options = LapinPublishOptions>, Marker>`, so the options type is
what the compiler matches and the body still names no publisher type.

A service that speaks to more than one broker keeps a routes file per broker and writes the
prefixed originals ([`LapinPublish`], [`ConfirmsPublish`], [`LapinRequest`]) where two of them meet.

# The `AsyncAPI` document

The `asyncapi` feature fills the AMQP 0.9.1 half of the document a service prints with
`cargo run -- asyncapi gen`; without it the document is generated all the same, with nothing
`RabbitMQ`-specific in it. Everything is computed from a descriptor or a policy alone, because the
document is built before anything connects.

A subscription's channel is the queue, and its binding carries the durability, exclusivity and
auto-delete the descriptor states; its receive operation reports `ack: true`, because this crate
always acknowledges by hand. A publish policy describes the channel from the other side: a routing
key on a named exchange, or a queue where the policy is on the default exchange. The name it
carries is the destination the mount site resolved - a reply's own name, a slot's name, a
`dead_letter(..)` declaration - because a policy carries publish settings and never a destination.
Its send operation carries the properties every message through the policy takes - the delivery
mode, and the priority and expiration where the policy fixes them - because the document describes
the declaration and not a call, so a per-message step changes nothing in it. A handler mounted
with [`DirectReplyTo`] has no reply address to report, so the operation names where a client reads
one instead, `$message.header#/reply-to`.

What binds a queue to its exchanges has no place in the specification's binding at all: a channel
there is a queue or a routing key, and nothing describes what joins the two - no exchange, no
binding key, no arguments. The request for them, `asyncapi/bindings#263`, was closed as not
planned. So a subscription reports its bindings in this crate's own extension beside the `amqp`
one, `x-ruststream-amqp`, as a `bindings` list in declaration order, one entry per `bind` and
`bind_with`:

```json
"x-ruststream-amqp": {
  "bindings": [
    { "exchange": "events", "type": "topic", "routingKey": "inventory.*" },
    {
      "exchange": "attributes",
      "type": "headers",
      "routingKey": "",
      "arguments": { "tier": "gold", "x-match": "all" }
    }
  ]
}
```

The routing key is written even when it is empty, because an empty key is a binding key like any
other; the arguments are left out where there are none; and a subscription that declares no
binding carries no extension. An extension is not a binding, so it has no `bindingVersion`.

The server entry reports the host and the AMQP version behind it. It never carries what the
connection URI holds: the document is published and shared, so the credentials and the virtual host
are dropped. Two fields of the specification's binding stay empty for want of an honest source -
the virtual host, which no descriptor or policy sees, and the message type, which the document
already reports as the message's own name and which a binding hook never learns, being handed the
subscription or the destination and nothing of what travels over it.

# Testing

A test runs the service's own app: the builder `main` runs, on [`LapinBroker`], handed to the
framework's `TestApp` harness unchanged. With the `testing` feature in `[dev-dependencies]`,
`TestApp::start` connects the broker in process instead of dialling the server, and the test
addresses it by its production type, `tb.broker::<LapinBroker>()`. `TestApp::start_live` runs the
same test body against a running `RabbitMQ`. The harness's usage is the core's:
<https://docs.rs/ruststream/latest/ruststream/testing/index.html>.

```
# #[cfg(feature = "testing")]
# mod demo {
use ruststream::testing::TestApp;
use ruststream_lapin::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq, Outgoing)]
#[outgoing(name = "payments")]
struct Payment {
    amount: u64,
}

#[subscriber(RabbitQueue::new("payments"))]
async fn accept(payment: &Payment) -> HandlerOutcome {
    if payment.amount == 0 {
        return HandlerOutcome::drop();
    }
    HandlerOutcome::ack()
}

/// The app `main` runs.
pub fn app() -> RustStream {
    RustStream::new(AppInfo::new("payments", "0.1.0"))
        .with_broker(LapinBroker::new("amqp://rabbit:5672"), |b| {
            b.include(accept);
        })
}

pub async fn accepts_a_payment() -> Result<(), Box<dyn std::error::Error>> {
    let tb = TestApp::start(app()).await?;

    // The publish returns once the handler it woke has settled.
    tb.broker::<LapinBroker>()
        .message(&Payment { amount: 100 })
        .publish()
        .await?;

    tb.broker::<LapinBroker>()
        .subscriber("payments")
        .assert_called_once()
        .with(&Payment { amount: 100 })
        .settled(HandlerOutcome::ack());

    tb.shutdown().await?;
    Ok(())
}
# }
# fn main() {}
```

In process, the broker's connected form carries an in-process transport in place of the
connection, and so do its subscribers, its publishers and its deliveries: the descriptors and
publish policies of the routes file are the production ones, and a mount that does not compile
against the server does not compile in process either. The transport has no settings of its own.
It reads the broker's: [`declare_topology`](LapinBroker::declare_topology) decides whether a
subscription declares its queue and whether a quorum queue can take a mount site's retry
declaration, exactly as on the server.

It never succeeds where the server fails. A publish is framed by the same conversion a live one
goes through, so a name or a header the protocol cannot carry is refused; a declaration the server
refuses (a transient queue that is not exclusive, a binding on the default exchange, a queue
declared again with other settings, a quorum queue with a priority) is refused; and a delivery
settled after its connection closed reports the closed channel. The default exchange and the
direct, topic, fanout and headers exchanges route through the bindings the descriptors describe;
competing consumers share a queue; a requeue goes back through the queue; a quorum queue counts
deliveries and dead-letters a spent one through its route; a waiting queue releases a delayed
copy; and direct reply-to gives every request an address of its own.

What only a server has belongs to the live mode, over the same test body: a queue's storage while
nobody consumes it (in process a message no consumer takes is dropped), the prefetch window,
publisher confirms, the atomicity of a server transaction, a plugin exchange's routing, and the
topology a service expects to find without declaring it (in process every queue a subscription
names is there). The crate's live suites run against the stand in `docker-compose.test.yml`
(`just test-brokers`).

# Operations

* The connection URI carries the credentials, the virtual host and the scheme:
  `amqp://user:pass@host:5672/vhost`, or `amqps://` with one of the TLS features
  (`tls-native-tls`, `tls-rustls`, `tls-rustls-ring`, mapped onto `lapin`'s own).
* [`connection_name`](LapinBroker::connection_name) is what the management UI shows for this
  service.
* [`prefetch`](LapinBroker::prefetch) caps unacknowledged deliveries in flight per subscription,
  which is how back-pressure reaches the server; a descriptor overrides it per queue. It is a
  `NonZeroU16` because AMQP reads `basic.qos(0)` as "no limit", so leaving it unset is how
  unlimited is spelled.
* [`declare_topology`](LapinBroker::declare_topology) is off by default: descriptors state the
  expected topology, a missing queue is a subscribe error, and the infrastructure is yours. With it
  on, subscribing declares the bound exchanges (the built-in `amq.*` ones and the default exchange
  aside), the queue and the bindings, in that order. Declaration is idempotent while the descriptor
  matches; the server rejects a redeclare with different properties (`PRECONDITION_FAILED`).
* One connection carries the service. Each subscription opens a channel of its own, fire-and-forget
  publishes share one channel, and a confirms or server-transaction publisher opens its own on
  first use, because both modes are channel state.
* `RabbitMQ` 4 denies transient non-exclusive queues, so a classic queue declared
  `durable(false)` has to be [`exclusive`](RabbitQueue::exclusive) too.
* The plugin features (`plugin-consistent-hash`, `plugin-dme`) are off by default and need their
  plugin enabled on the server.
* Handles that outlive the connection report `AmqpError::Closed` rather than succeeding quietly: a
  publisher handed out before shutdown is aliased, and the typed ladder can only rule out misuse
  through the owner's own handle.

[`lapin`]: https://docs.rs/lapin
