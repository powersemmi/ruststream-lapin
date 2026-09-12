<h1 align="center">ruststream-lapin</h1>

<p align="center">
  <i>The RabbitMQ / AMQP 0.9.1 broker for the <a href="https://github.com/powersemmi/ruststream">RustStream</a> messaging framework: native per-message acknowledgement, quorum queues, publisher confirms, direct reply-to, and an in-process test broker.</i>
</p>

<p align="center">
  <a href="https://github.com/powersemmi/ruststream-lapin/actions/workflows/ci.yml"><img src="https://github.com/powersemmi/ruststream-lapin/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://crates.io/crates/ruststream-lapin"><img src="https://img.shields.io/crates/v/ruststream-lapin.svg" alt="crates.io"></a>
  <a href="https://crates.io/crates/ruststream-lapin"><img src="https://img.shields.io/crates/dr/ruststream-lapin" alt="Recent downloads"></a>
  <a href="https://docs.rs/ruststream-lapin"><img src="https://img.shields.io/docsrs/ruststream-lapin" alt="docs.rs"></a>
  <img src="https://img.shields.io/badge/MSRV-1.88-blue.svg" alt="MSRV 1.88">
  <img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="License">
</p>

<p align="center">
  <b><a href="https://powersemmi.github.io/ruststream-lapin/">Documentation</a></b>
</p>

---

`ruststream-lapin` implements the RustStream broker contract over [`lapin`](https://crates.io/crates/lapin), the mature AMQP 0.9.1 client. Handlers, routers, codecs, and middleware come from the framework; this crate supplies the transport - and nothing broker-specific leaks back into the framework.

## Features

- **Native settlement.** AMQP has per-message acknowledgement built in:
  `ack` is `basic.ack`, retry is `basic.nack(requeue = true)`, drop is
  `basic.reject(requeue = false)` - straight into the queue's dead-letter exchange when one is
  configured.
- **Descriptors for real topology.** `RabbitQueue` carries durability, queue type
  (`Classic` / `Quorum`), exchange bindings, prefetch, dead-letter and raw `x-*` arguments; the
  bare-string `#[subscriber("orders")]` form consumes the queue with that name.
- **Infrastructure stays yours.** Descriptors describe the EXPECTED topology; nothing is created
  on the broker unless the service opts in with `.declare_topology(true)`.
- **Batches assembled on the client.** AMQP pushes one `basic.deliver` at a time, so there is no
  wire batch to ask the broker for: a batch handler names its size at the mount site with
  `.batch(nonzero!(n))` and this crate fills it from the delivery stream. The descriptor governs
  how it forms - a prefetch window at least as wide as the batch, and `.batch_wait(..)` capping
  how long a batch that never fills keeps its deliveries.
- **Durable delayed retry.** `RabbitQueue::delay(..)` makes `retry_after` native and keeps the
  delayed copy on the broker instead of the core in-process fallback. Two backends:
  `Delay::dlx_ttl()` routes through a TTL waiting queue that dead-letters back to the origin, and
  `Delay::plugin_dme()` (behind `plugin-dme`) through the delayed-message exchange, where mixed
  delays do not block each other.
- **Three publishers, chosen on the policy.** `LapinPublish::default()` is fire-and-forget;
  `.confirms()` awaits every broker confirm and buffers a transaction client-side (durable, fast,
  recommended, and the policy the family's `TransactionalPublish` name points at); `.server_tx()`
  uses AMQP channel transactions for atomic visibility at commit.
- **Owned and borrowed transactions.** Confirms buffer client-side, so the confirms publisher
  also offers the owned kind: `owned_transaction()` hands back a value owning its buffer, so any
  number can be open on one handle, settling one never touches another, and the handle keeps
  publishing directly meanwhile. `server_tx` keeps only the borrowed kind - `tx.select` is
  channel state, one per channel.
- **Per-message AMQP properties.** The `priority`, the per-message `expiration` (TTL) and the
  delivery mode are typed settings (`LapinPublishOptions`), never headers. The mount site fixes
  them for every message on the policy (`Publish::default().priority(3)`), and the publish builder
  adjusts one message with `.priority(9)`, `.expiration(ttl)` or `.persistent(false)`. A step is a
  position on the builder rather than a wrapper around the publisher, so it keeps the codec and
  the destination the mount site gave it, holds through both transaction kinds, and stays
  attributed to its `Out` slot under the test harness. Written into the header table instead, a
  value would reach the broker as a table entry it reads for no purpose.
- **Request/reply over direct reply-to.** `LapinRequest` pairs into a requester implementing the
  framework's `RequestReply` capability on `amq.rabbitmq.reply-to` with correlation-id
  multiplexing. The responder side is a mount-site transform, `DirectReplyTo`, so the handler
  stays a plain request-to-reply function.
- **A typed lifecycle.** `LapinBroker::new(uri)` is synchronous and does no I/O, so the broker
  composes with `#[ruststream::app]`; connecting consumes it into `ConnectedLapinBroker` and
  shutting that down consumes it again, so subscribing before connect or publishing after
  shutdown does not compile. Publishers are policies that hold no connection and pair with the
  connected broker at startup.
- **In-process test broker.** The `testing` feature ships `LapinTestBroker`, an in-process
  stand-in for RabbitMQ that plugs into the framework's `TestApp` harness, so handlers are
  unit-tested with the same wiring they ship with - no server needed.

## Install

```toml
[dependencies]
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-lapin = "0.7"
serde = { version = "1", features = ["derive"] }

[dev-dependencies]
ruststream-lapin = { version = "0.7", features = ["testing"] }
```

TLS (`amqps://`) is feature-gated, mapped onto `lapin`'s backends: `tls-rustls`,
`tls-rustls-ring`, `tls-native-tls`. Two features need a plugin enabled on the broker and are off
by default: `plugin-consistent-hash` adds `RabbitExchange::consistent_hash(..)` for server-side
hash fan-out, and `plugin-dme` adds the delayed-message-exchange backend for `RabbitQueue::delay`.

## Write a service

A handler body names capabilities, never a broker type: bounded `Out<impl Publisher>`, the handler
below mounts unchanged on RabbitMQ and on the in-process test broker. The routes name values, and
this crate's prelude spells its policies with the family's uniform names - `Publish`,
`TransactionalPublish`, `Request` - so a router reads the same whichever broker it targets.

```rust
use ruststream_lapin::prelude::*;
use serde::{Deserialize, Serialize};

// `PartialEq` and `Serialize` are here for the test below, which publishes an order and
// asserts on the decoded one.
#[derive(Debug, Deserialize, PartialEq, Serialize)]
struct Order {
    id: u64,
}

#[derive(Debug, Deserialize, Outgoing, PartialEq, Serialize)]
#[outgoing(name = "receipts")]
struct Receipt {
    order_id: u64,
}

#[subscriber(RabbitQueue::new("orders").durable(true))]
async fn settle(order: &Order, Out(receipts): Out<impl Publisher>) -> HandlerOutcome {
    let receipt = Receipt { order_id: order.id };
    if receipts.message(&receipt).publish().await.is_err() {
        // Nothing reached the broker; ask for redelivery and settle the order again.
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    let broker = LapinBroker::new("amqp://localhost:5672").prefetch(nonzero!(64));
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(settle).out(DefaultSlot, Publish::default()).build();
    })
}
```

`.out(marker, policy)` is the one mount verb: `DefaultSlot` names the handler's single unnamed
`Out` slot, `Reply` the publisher a `#[subscriber(.., publish("dest"))]` handler's return value
leaves through. The message name is the routing key, sent to the policy's exchange, and on the
default exchange it addresses the queue with that name - which is why this service needs no
topology. `#[ruststream::app]` generates `main`, so the binary understands `run` and
`asyncapi gen` with no boilerplate.

## Test it

The same handler and the same mount chain, against the in-process broker: no server, and the
routes file does not change - the crate's own policies pair here too.

```rust
use ruststream::testing::TestApp;
use ruststream_lapin::testing::LapinTestBroker;

let service = RustStream::new(AppInfo::new("orders", "0.1.0"))
    .with_broker(LapinTestBroker::new(), |b| {
        b.include(settle).out(DefaultSlot, Publish::default()).build();
    });
let tb = TestApp::start(service).await?;

// `publish` returns once the handlers it woke have settled.
tb.broker::<LapinTestBroker>()
    .publish("orders", &Order { id: 42 })
    .await?;

tb.broker::<LapinTestBroker>()
    .subscriber("orders")
    .assert_called_once()
    .with(&Order { id: 42 })
    .settled(HandlerOutcome::ack());

tb.broker::<LapinTestBroker>()
    .published::<Receipt>("receipts")
    .assert_called_once()
    .with(&Receipt { order_id: 42 });
```

Exchange routing, bindings, dead-lettering, prefetch and publisher confirms are server behaviour:
the env-gated suite exercises those against a real RabbitMQ (`just test-brokers`).

## Scaffold a service

Generate a runnable starter with [`cargo generate`](https://github.com/cargo-generate/cargo-generate):

```bash
# work queue (default exchange, competing consumers)
cargo generate --git https://github.com/powersemmi/ruststream-lapin templates/amqp-queue --name my-service
# topic exchange (routing-key patterns, declared topology)
cargo generate --git https://github.com/powersemmi/ruststream-lapin templates/amqp-topic --name my-service
```

## Contributing

```bash
just check          # fmt, clippy, feature checks
just test           # in-process tests, no server
just test-brokers   # live integration and conformance against a RabbitMQ container
just test-plugins   # the plugin-gated tests against the plugin-enabled container
```

## License

Licensed under the [Apache-2.0](./LICENSE) license.
