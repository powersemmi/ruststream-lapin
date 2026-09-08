# Testing

The `testing` feature ships `LapinTestBroker`, an in-process stand-in for RabbitMQ in
application tests: the same handlers, descriptors, and wiring, no server. It follows the same
ladder as the real broker (synchronous `new`, consuming `connect`, consuming `shutdown`), routes
by exact queue name (the default-exchange model), records every publish, and plugs into the
framework's `TestApp` harness, which drives each publish to quiescence so assertions never race
the handlers.

```toml
[dev-dependencies]
ruststream-lapin = { version = "0.7", features = ["testing"] }
```

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_testing.rs:handler"
```

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_testing.rs:testapp"
```

[Batches](queues.md#batches) come along: the transport assembles them on the client exactly as the
real subscriber does, so a `&[T]` handler mounts on the test broker unchanged and
`assert_batch_sizes(..)` reports the batches the body was handed.

Delivery metadata comes along too: a handler reading AMQP fields through
[`AmqpContext`](queues.md#delivery-metadata) - as `Ctx<RoutingKey>` extractors or a
`ctx: &mut Context<'_, AmqpContext>` parameter - mounts on the test broker unchanged. The
transport reports those fields against its own model: the default exchange, the queue name as the
routing key, a delivery tag numbered per subscription, and `redelivered` set on a requeue.

## What the test broker does not simulate

Exchange types, bindings, dead-lettering, prefetch, and request/reply are transport behavior;
exercise them against a real server. The crate's own integration tests run that way, gated on
`AMQP_TEST_URL`:

```text
just brokers-up
AMQP_TEST_URL=amqp://127.0.0.1:5672 cargo test --workspace --all-features -- --test-threads=1
```
