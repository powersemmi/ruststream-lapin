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
handler mountable on a real broker and on the in-process test broker. One body imports this
crate's prelude too: one that adjusts an
[AMQP property][properties] for a single message. It names the
settings type rather than a publisher type - `Out<impl Publisher<Options = LapinPublishOptions>>` -
so the handler stays mountable on both.

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
  `orders`, and the [`RabbitQueue`][subscribing] descriptor adds bindings, queue types and
  prefetch. A batch handler names its size at the mount site, and the crate assembles the batch
  on the client, since AMQP has no wire batch; see [Batches][batches].
- On the publish side the message name is the routing key, and the exchange belongs to the
  publish policy: the default exchange unless you name another. See [Publishing][publishing].
- Settlement is native: `ack` sends `basic.ack`, retry sends `basic.reject(requeue = true)`, drop
  sends `basic.reject(requeue = false)`, which dead-letters the message when the queue has a
  dead-letter exchange. A rejection is the frame RabbitMQ counts as a spent delivery, so a
  handler's retry runs down a quorum queue's own limit.
- Nothing is declared on the broker unless you opt in with `.declare_topology(true)`: the
  topology is yours to manage.

## Capabilities

Which of the framework's optional capability traits this broker implements natively:

| Capability | Native | Notes |
| --- | --- | --- |
| `Subscribe` | yes | Consumes the queue the subscription names; [`RabbitQueue`](https://docs.rs/ruststream-lapin/latest/ruststream_lapin/index.html#subscribing) adds bindings, queue type, and prefetch. |
| `BatchSubscriber` | yes, client-side | AMQP pushes one `basic.deliver` at a time, so the batch is assembled on the client, to the size the mount site names, under the descriptor's prefetch window and `batch_wait`. See [Batches](https://docs.rs/ruststream-lapin/latest/ruststream_lapin/index.html#batches). |
| `TransactionalPublisher` | yes | Both transactional publishers: `.confirms()` buffers client-side and awaits every confirm on commit, `.server_tx()` uses AMQP channel transactions. See [Publishing](https://docs.rs/ruststream-lapin/latest/ruststream_lapin/index.html#publishing). |
| `OwnedTransactions` | yes (confirms only) | A confirms transaction is a client-side buffer, so any number can be open on one handle. `server_tx` puts the channel itself into transactional mode, which is channel state with exactly one instance. |
| `RequestReply` | yes | `LapinRequest` pairs into a requester over direct reply-to with correlation-id multiplexing. See [Request/reply](https://docs.rs/ruststream-lapin/latest/ruststream_lapin/index.html#request-and-reply). |
| `Partitioned` | yes | The producer sets the key in the `amqp-partition-key` header and the runtime's worker lanes read it; AMQP itself does not interpret it. See [Delivery metadata](https://docs.rs/ruststream-lapin/latest/ruststream_lapin/index.html#delivery-metadata). |
| `Seekable` + `Positioned` | no | A queue keeps no history to reposition into. |
| `DescribeServer` | yes | Reports the connection host and the AMQP version behind it, which is what the AsyncAPI document records. See [The AsyncAPI document](https://docs.rs/ruststream-lapin/latest/ruststream_lapin/index.html#the-asyncapi-document). |

## Scaffold a service

Generate a runnable starter with [`cargo generate`](https://github.com/cargo-generate/cargo-generate),
one template per messaging shape:

```bash
cargo generate --git https://github.com/powersemmi/ruststream-lapin templates/amqp-queue
cargo generate --git https://github.com/powersemmi/ruststream-lapin templates/amqp-topic
```

## Documentation

The crate documents itself on docs.rs, and that overview is the guide:

- [Subscribing][subscribing] - the two queue descriptors, queue types, bindings, prefetch, the
  retry cap, delayed redelivery, batches and delivery metadata.
- [Publishing][publishing] - the routing model, the policies, the per-message AMQP properties,
  replies and transactions.
- [Request/reply][request-reply] - RPC over direct reply-to.
- [The AsyncAPI document][documenting] - the AMQP bindings a service reports about itself.
- [Testing][testing] - the in-process test broker under the `TestApp` harness.
- [Operations][operations] - TLS, connection settings, opt-in declaration, known gaps.

Installation, the tutorial and the other brokers are on the RustStream site:
<https://powersemmi.github.io/ruststream/>.

[subscribing]: https://docs.rs/ruststream-lapin/latest/ruststream_lapin/index.html#subscribing
[batches]: https://docs.rs/ruststream-lapin/latest/ruststream_lapin/index.html#batches
[publishing]: https://docs.rs/ruststream-lapin/latest/ruststream_lapin/index.html#publishing
[properties]: https://docs.rs/ruststream-lapin/latest/ruststream_lapin/index.html#per-message-properties
[request-reply]: https://docs.rs/ruststream-lapin/latest/ruststream_lapin/index.html#request-and-reply
[documenting]: https://docs.rs/ruststream-lapin/latest/ruststream_lapin/index.html#the-asyncapi-document
[testing]: https://docs.rs/ruststream-lapin/latest/ruststream_lapin/index.html#testing
[operations]: https://docs.rs/ruststream-lapin/latest/ruststream_lapin/index.html#operations
