# RabbitMQ broker

`ruststream-lapin` runs a RustStream service on RabbitMQ over [`lapin`](https://docs.rs/lapin),
the AMQP 0.9.1 client. A RabbitMQ queue is destructive storage: a delivery leaves the queue once
it is acknowledged. Acknowledgement, requeue and dead-lettering are protocol frames, so the
framework settles a message on the broker itself.

```toml
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-lapin = "0.7"
serde = { version = "1", features = ["derive"] }
```

`LapinBroker::new` does no I/O, so a RabbitMQ service is assembled with the same
`#[ruststream::app]` macro as any other broker. The runtime connects the broker at startup,
before it opens subscriptions. Connecting consumes the broker and yields `ConnectedLapinBroker`,
the only value carrying a subscribe or publish surface.

A handler body names capabilities: `Out<impl Publisher>`, `Out<impl TransactionalPublisher>`,
`Out<impl RequestReply>`. It imports the framework's prelude alone, which is what keeps one
handler mountable on a real broker and on the in-process test broker.

A routes file names values and imports `ruststream_lapin::prelude::*`, which brings the
framework's prelude with it. It adds the family's uniform mount-site names: `Publish`,
`TransactionalPublish` and `Request`, aliases for `LapinPublish`, `ConfirmsPublish` and
`LapinRequest`.

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_quickstart.rs:handler"
```

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_quickstart.rs:app"
```

## The transport model

- A subscription consumes one queue. `#[subscriber("orders")]` consumes the queue named
  `orders`, and the [`RabbitQueue`](queues.md) descriptor adds bindings, queue types and
  prefetch. A batch handler names its size at the mount site, and the crate assembles the batch
  on the client, since AMQP has no wire batch; see [Batches](queues.md#batches).
- On the publish side the message name is the routing key, and the exchange belongs to the
  publish policy: the default exchange unless you name another. See [Publishing](publishing.md).
- Settlement is native: `ack` sends `basic.ack`, retry sends `basic.nack(requeue = true)`, drop
  sends `basic.reject(requeue = false)`, which dead-letters the message when the queue has a
  dead-letter exchange.
- Nothing is declared on the broker unless you opt in with `.declare_topology(true)`: the
  topology is yours to manage.

## Capabilities

Which of the framework's optional capability traits this broker implements natively:

| Capability | Native | Notes |
| --- | --- | --- |
| `Subscribe` | yes | Consumes the queue the subscription names; [`RabbitQueue`](queues.md) adds bindings, queue type, and prefetch. |
| `BatchSubscriber` | yes, client-side | AMQP pushes one `basic.deliver` at a time, so the batch is assembled on the client, to the size the mount site names, under the descriptor's prefetch window and `batch_wait`. See [Batches](queues.md#batches). |
| `TransactionalPublisher` | yes | Both transactional publishers: `.confirms()` buffers client-side and awaits every confirm on commit, `.server_tx()` uses AMQP channel transactions. See [Three publishers](publishing.md#three-publishers). |
| `OwnedTransactions` | yes (confirms only) | A confirms transaction is a client-side buffer, so any number can be open on one handle. `server_tx` puts the channel itself into transactional mode, which is channel state with exactly one instance. |
| `RequestReply` | yes | `LapinRequest` pairs into a requester over direct reply-to with correlation-id multiplexing. See [Request/reply](request-reply.md). |
| `Partitioned` | yes | The producer sets the key in the `amqp-partition-key` header and the runtime's worker lanes read it; AMQP itself does not interpret it. See [Keyed worker lanes](queues.md#keyed-worker-lanes). |
| `Seekable` + `Positioned` | no | A queue keeps no history to reposition into. |
| `DescribeServer` | yes | Reports the connection host, which is what the AsyncAPI document records. |

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
- [Testing](testing.md) - the in-process test broker under the `TestApp` harness.
