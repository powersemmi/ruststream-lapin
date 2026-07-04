<h1 align="center">ruststream-lapin</h1>

<p align="center">
  <i>The RabbitMQ / AMQP 0.9.1 broker for the <a href="https://github.com/powersemmi/ruststream">RustStream</a> messaging framework: native per-message acknowledgement, quorum queues, publisher confirms, and an in-process test broker.</i>
</p>

<p align="center">
  <a href="https://github.com/powersemmi/ruststream-lapin/actions/workflows/ci.yml"><img src="https://github.com/powersemmi/ruststream-lapin/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://crates.io/crates/ruststream-lapin"><img src="https://img.shields.io/crates/v/ruststream-lapin.svg" alt="crates.io"></a>
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

- **Native settlement, no republish tricks.** AMQP has per-message acknowledgement built in:
  `ack` is `basic.ack`, retry is `basic.nack(requeue = true)`, drop is
  `basic.reject(requeue = false)` - straight into the queue's dead-letter exchange when one is
  configured.
- **Descriptors for real topology.** `RabbitQueue` carries durability, queue type
  (`classic` / `quorum`), exchange bindings, prefetch, dead-letter and raw `x-*` arguments; the
  bare-string `#[subscriber("orders")]` form consumes the queue with that name.
- **Infrastructure stays yours.** Descriptors describe the EXPECTED topology; nothing is created
  on the broker unless the service opts in with `.declare_topology(true)`.
- **Durable delayed retry.** `.delay(..)` routes `retry_after` through a broker TTL waiting queue
  that dead-letters back to the origin, keeping the delayed copy on the broker instead of the
  core in-process fallback.
- **Two transactional publishers, chosen on the publisher.** `.confirms()` buffers and awaits
  every broker confirm on commit (durable, fast, recommended); `.server_tx()` uses AMQP channel
  transactions for server-side atomicity.
- **Request/reply over direct reply-to.** `broker.requester()` implements the framework's
  `RequestReply` capability on `amq.rabbitmq.reply-to` with correlation-id multiplexing.
- **Lazy startup contract.** `LapinBroker::new(uri)` is synchronous and does no I/O; the runtime
  connects once at startup, so the broker composes with `#[ruststream::app]`.
- **In-process test broker.** The `testing` feature ships `LapinTestBroker`, an in-process
  stand-in for RabbitMQ that plugs into the framework's `TestApp` harness, so handlers are
  unit-tested with the same wiring they ship with - no server needed.

## Install

```toml
[dependencies]
ruststream = { version = "0.5", features = ["macros", "json"] }
ruststream-lapin = "0.5"
serde = { version = "1", features = ["derive"] }
```

TLS (`amqps://`) is feature-gated, mapped onto `lapin`'s backends: `tls-rustls`,
`tls-rustls-ring`, `tls-native-tls`.

## License

Apache-2.0.
