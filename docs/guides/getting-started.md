---
sidebar_position: 1
description: "Install ruststream-lapin, start a local RabbitMQ, run a first subscriber against it and test it without a server."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Getting started

This guide installs `ruststream-lapin`, starts a local RabbitMQ and runs a first service against it.

## Installation

```toml title="Cargo.toml"
[dependencies]
ruststream = { version = "0.7", features = ["macros", "json", "asyncapi", "logging"] }
ruststream-lapin = "0.7"
serde = { version = "1", features = ["derive"] }
tracing = "0.1"

[dev-dependencies]
ruststream-lapin = { version = "0.7", features = ["testing"] }
tokio = { version = "1", features = ["macros", "rt"] }
```

`ruststream` is the framework: `macros` gives the attributes, `json` the default codec, `asyncapi` the service's AsyncAPI document, and `logging` prints the service's log to the terminal.
`ruststream-lapin` is the RabbitMQ broker, built on [lapin](https://docs.rs/lapin), an AMQP 0.9.1 client written in Rust.
The build needs nothing outside Cargo.
Its `testing` feature adds the [in-process RabbitMQ](../components/testing/index.md) that tests run on.

The broker crate has these features:

| Feature | What it gives |
|---|---|
| `asyncapi` | the [AMQP bindings](../components/asyncapi/index.md) in the generated AsyncAPI document |
| `testing` | the in-process RabbitMQ behind `TestApp` |
| `tls-native-tls` | [TLS](../components/broker/tls.md) through the platform's TLS library |
| `tls-rustls` | TLS through rustls |
| `tls-rustls-ring` | TLS through rustls with the `ring` crypto provider |
| `plugin-consistent-hash` | the [consistent-hash exchange](../components/exchanges/consistent-hash.md) of the RabbitMQ plugin |
| `plugin-dme` | a [delayed retry](../components/settlement/plugin-dme.md) through the delayed-message-exchange plugin |

## A local RabbitMQ

One RabbitMQ node with the management UI is enough to develop against:

```yaml title="compose.yaml"
services:
  rabbitmq:
    image: rabbitmq:4-management-alpine
    ports:
      - "127.0.0.1:5672:5672"
      - "127.0.0.1:15672:15672"
```

```bash
docker compose up -d
```

The broker listens on `localhost:5672`.
The management UI is at `http://localhost:15672`, user `guest`, password `guest`.
RabbitMQ accepts the `guest` account only from localhost, which is how the ports are published.

## The first service

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/getting_started.rs#service"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/getting_started.rs#service"
```

</TabItem>
</Tabs>

`take_order` reads JSON orders from the `orders` queue.
It acknowledges an order with an amount and drops one without.
[`LapinBroker::new`](../components/broker/index.md) takes the connection URI and only records it.
The connection opens when the service starts.
[`declare_topology(true)`](../components/broker/declare-topology.md) lets the service declare the queues it reads, so it starts on an empty RabbitMQ.
`#[ruststream::app]` turns `app` into `main`; in the manual form, [`run_main`](api:ruststream::runtime::cli::run_main) does the same.

Run the service:

```bash
cargo run -- run
```

Publish an order from another terminal with `rabbitmqadmin`, which ships in the image:

```bash
docker compose exec rabbitmq rabbitmqadmin publish message \
  --routing-key orders --payload '{"id":1,"amount_cents":1250}'
```

The message goes to the default exchange, which delivers it to the queue named by the routing key.
The service logs `order taken` with the order's id.
The `orders` queue's page in the management UI publishes the same message.
<kbd>Ctrl</kbd>+<kbd>C</kbd> stops the service.

The service also describes itself:

```bash
cargo run -- asyncapi gen
```

The command prints the service's AsyncAPI document: the server, the `orders` channel and the `Order` message.

`cargo generate` scaffolds the same kind of service with a router and a reply:

```bash
cargo generate --git https://github.com/powersemmi/ruststream-lapin templates/amqp-queue --name my-service
```

The `templates/amqp-topic` template starts from a topic exchange instead.

## The first test

The example file also holds two tests.
Each starts the same `app` with `TestApp::start`, publishes an order and checks how `take_order` settled it.
`LapinBroker` connects to an in-process RabbitMQ there, so the tests need no server:

```bash
cargo test
```

[Testing a RabbitMQ service](testing.md) walks through these tests and what the in-process RabbitMQ models.

## Coming from lapin

A service on lapin opens its own channels and writes its own consumer loop.
Here is where each part goes in RustStream:

| lapin | RustStream |
|---|---|
| `Connection::connect` and `create_channel` | [`LapinBroker::new`](../components/broker/connection.md); the runtime connects at startup |
| `channel.basic_qos` | [`prefetch`](../components/subscriptions/prefetch.md) on the broker or on one queue |
| `queue_declare`, `exchange_declare`, `queue_bind` | a [queue descriptor](../components/subscriptions/rabbit-queue.md) and [`declare_topology(true)`](../components/broker/declare-topology.md) ([Topology](topology.md)) |
| `basic_consume` and a `while let Some(delivery)` loop | a `#[subscriber]` handler |
| deserializing `delivery.data` | the handler's parameter type, decoded by the codec |
| `delivery.ack` and `delivery.reject` | the handler's [outcome](../components/settlement/index.md) ([Acknowledgements, retries and dead letters](retries.md)) |
| `basic_publish` | a reply or an `Out` parameter, sent through a [publish policy](../components/publishing/index.md) |
| `confirm_select` and waiting for confirms | [`ConfirmsPublish`](../components/transactions/confirms-publish.md) ([Reliable publishing](publishing.md)) |
| a callback queue and a correlation table | [`LapinRequest`](../components/request/index.md) ([Request and reply](rpc.md)) |
