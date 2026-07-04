# RabbitMQ broker

`ruststream-lapin` is the RabbitMQ / AMQP 0.9.1 broker for the
[RustStream](https://powersemmi.github.io/ruststream/) framework, backed by
[`lapin`](https://docs.rs/lapin). AMQP fits the framework's settlement contract natively: acks,
requeues, and dead-lettering are protocol frames, not republish workarounds. An in-process test
broker ships under the `testing` feature.

```toml
ruststream = { version = "0.5", features = ["macros", "json"] }
ruststream-lapin = "0.5"
serde = { version = "1", features = ["derive"] }
```

`LapinBroker::new` is synchronous and does no I/O, so a RabbitMQ service is assembled with the
same `#[ruststream::app]` macro as any other broker. The runtime connects the broker once at
startup, before opening subscriptions.

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_quickstart.rs:handler"
```

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_quickstart.rs:app"
```

## The transport model

- A subscription consumes one queue; the bare-string form `#[subscriber("orders")]` consumes the
  queue named `orders`, and the [`RabbitQueue`](queues.md) descriptor adds bindings, queue types,
  and prefetch.
- On the publish side the message name is the routing key; the exchange is a property of the
  publisher (the default exchange unless configured). See [Publishing](publishing.md).
- Settlement is native: `ack` sends `basic.ack`, retry sends `basic.nack(requeue = true)`, drop
  sends `basic.reject(requeue = false)` - which dead-letters when the queue has a dead-letter
  exchange.
- Nothing is declared on the broker unless the service opts in with `.declare_topology(true)`:
  infrastructure stays the user's job.

## Scaffold a service

Generate a runnable starter with [`cargo generate`](https://github.com/cargo-generate/cargo-generate),
one template per messaging shape:

```bash
cargo generate --git https://github.com/powersemmi/ruststream-lapin templates/amqp-queue
cargo generate --git https://github.com/powersemmi/ruststream-lapin templates/amqp-topic
```

## Guides

- [Queues and topology](queues.md) - descriptors, queue types, bindings, prefetch, dead-letter,
  opt-in declaration.
- [Publishing](publishing.md) - the routing model, persistence, publisher confirms, and server
  transactions.
- [Request/reply](request-reply.md) - RPC over RabbitMQ direct reply-to.
- [Testing](testing.md) - the in-process test broker and the conformance harness.
