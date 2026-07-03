# ruststream-lapin

RabbitMQ / AMQP 0.9.1 broker implementation for the [RustStream](../..) messaging framework,
backed by [`lapin`](https://crates.io/crates/lapin). Native per-message settlement (`basic.ack` /
`basic.nack` / `basic.reject` with dead-lettering), queue and exchange descriptors with opt-in
topology declaration, publisher confirms and AMQP server transactions, and request/reply over
direct reply-to.

## Testing

```toml
[dev-dependencies]
ruststream-lapin = { version = "*", features = ["testing"] }
```

`features = ["testing"]` exposes an in-process test broker (a handler-stub transport with exact
queue-name routing) that drives the framework's `TestApp` harness. Never enable this feature in
production builds.
