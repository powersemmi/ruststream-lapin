<h1 align="center">ruststream-lapin</h1>

<p align="center">
  <i>The RabbitMQ / AMQP 0.9.1 broker for the <a href="https://github.com/powersemmi/ruststream">RustStream</a> messaging framework: native per-message acknowledgement, quorum queues, publisher confirms, direct reply-to, and an in-process mode that runs the production app in tests.</i>
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

`ruststream-lapin` connects a RustStream service to RabbitMQ over
[`lapin`](https://crates.io/crates/lapin), the AMQP 0.9.1 client. Handlers, routing, codecs and
middleware come from the framework; this crate is the transport.

## Features

- **Native settlement:** `ack` is `basic.ack`, a retry is `basic.reject` with requeue, and a drop
  goes to the queue's dead-letter exchange.
- **A descriptor per queue type:** `RabbitQueue` for classic queues, `RabbitQuorumQueue` for
  quorum queues, with bindings, prefetch and dead-letter settings.
- **Your topology:** nothing is created on the broker unless the service opts in with
  `declare_topology(true)`.
- **Durable delayed retry** through a TTL waiting queue or the delayed-message-exchange plugin, and
  a retry cap a quorum queue enforces itself.
- **Three publishers:** fire-and-forget, publisher confirms with client-side transactions, and AMQP
  channel transactions. Priority, TTL and persistence are typed per-message settings.
- **Request/reply** over direct reply-to.
- **AsyncAPI** with the specification's `amqp` bindings, behind the `asyncapi` feature.
- **Tests without a server:** the service's own app runs with `LapinBroker` in process.

## Install

```toml
[dependencies]
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-lapin = "0.7"
serde = { version = "1", features = ["derive"] }

[dev-dependencies]
ruststream-lapin = { version = "0.7", features = ["testing"] }
```

Optional features: `asyncapi`, TLS (`tls-rustls`, `tls-rustls-ring`, `tls-native-tls`), and the
broker plugins `plugin-consistent-hash` and `plugin-dme`.

## Write a service

```rust
use ruststream_lapin::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Outgoing, PartialEq, Serialize)]
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

`#[ruststream::app]` generates `main`, so the binary understands `run` and `asyncapi gen`.

Scaffold a fresh project from a template, a work queue or a topic exchange:

```bash
cargo generate --git https://github.com/powersemmi/ruststream-lapin templates/amqp-queue --name my-service
cargo generate --git https://github.com/powersemmi/ruststream-lapin templates/amqp-topic --name my-service
```

## Test it

`TestApp` runs the service's own app with `LapinBroker` in process, with no server.

```rust
use ruststream::testing::TestApp;

let tb = TestApp::start(app()).await?;

tb.broker::<LapinBroker>()
    .message(&Order { id: 42 })
    .to("orders")
    .publish()
    .await?;

tb.broker::<LapinBroker>()
    .subscriber("orders")
    .assert_called_once()
    .settled(HandlerOutcome::ack());

tb.broker::<LapinBroker>()
    .published::<Receipt>("receipts")
    .assert_called_once()
    .with(&Receipt { order_id: 42 });
```

`TestApp::start_live(app())` runs the same test against a running RabbitMQ.

## Documentation

- This crate: <https://docs.rs/ruststream-lapin>
- The framework: <https://powersemmi.github.io/ruststream/latest>

## Minimum supported Rust version

The MSRV is **1.88**, edition 2024.

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md).

## License

Licensed under the [Apache-2.0](./LICENSE) license.
