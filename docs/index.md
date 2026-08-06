# RabbitMQ broker

`ruststream-lapin` is the RabbitMQ / AMQP 0.9.1 broker for the
[RustStream](https://powersemmi.github.io/ruststream/) framework, backed by
[`lapin`](https://docs.rs/lapin). AMQP fits the framework's settlement contract natively: acks,
requeues, and dead-lettering are protocol frames rather than client-side republishing. An
in-process test broker ships under the `testing` feature.

```toml
ruststream = { version = "0.6", features = ["macros", "json"] }
ruststream-lapin = "0.6"
serde = { version = "1", features = ["derive"] }
```

`LapinBroker::new` is synchronous and does no I/O, so a RabbitMQ service is assembled with the
same `#[ruststream::app]` macro as any other broker. The runtime connects the broker once at
startup, before opening subscriptions; connecting consumes the broker and yields
`ConnectedLapinBroker`, the only value carrying a subscribe or publish surface.

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
  publish policy (the default exchange unless configured). See [Publishing](publishing.md).
- Settlement is native: `ack` sends `basic.ack`, retry sends `basic.nack(requeue = true)`, drop
  sends `basic.reject(requeue = false)` - which dead-letters when the queue has a dead-letter
  exchange.
- Nothing is declared on the broker unless the service opts in with `.declare_topology(true)`:
  infrastructure stays the user's job.

## Capabilities

Which of the framework's optional capability traits this broker implements natively:

| Capability | Native | Notes |
| --- | --- | --- |
| `Subscribe` | yes | Consumes the queue the subscription names; [`RabbitQueue`](queues.md) adds bindings, queue type, and prefetch. |
| `BatchSubscriber` | no | AMQP pushes one `basic.deliver` at a time, so there is no wire-level batch. [Prefetch](queues.md#prefetch) is the flow-control window instead. |
| `TransactionalPublisher` | yes | Both transactional publishers: `.confirms()` buffers client-side and awaits every confirm on commit, `.server_tx()` uses AMQP channel transactions. See [Three publishers](publishing.md#three-publishers). |
| `OwnedTransactions` | yes (confirms only) | A confirms transaction is a client-side buffer, so any number can be open on one handle. `server_tx` puts the channel itself into transactional mode, which is channel state with exactly one instance. |
| `RequestReply` | yes | `LapinRequest` pairs into a requester over direct reply-to with correlation-id multiplexing. See [Request/reply](request-reply.md). |
| `Partitioned` | yes | The key travels in the `amqp-partition-key` header and feeds the runtime's worker lanes; AMQP does not interpret it, so the producer sets it. See [Keyed worker lanes](queues.md#keyed-worker-lanes). |
| `Seekable` + `Positioned` | no | An AMQP queue is destructive: a delivery is removed from the queue when it is acked, so there is no retained history to reposition into. |
| `DescribeServer` | yes | Reports the configured AMQP address, which is what the AsyncAPI document records. |

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
