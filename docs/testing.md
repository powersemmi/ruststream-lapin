# Testing

Test a RabbitMQ service without a RabbitMQ server. The `testing` feature ships `LapinTestBroker`,
an in-process stand-in that runs the same handlers, descriptors and wiring as production. It
routes by exact queue name, the default-exchange model, and records every publish, so a test can
assert on what a handler sent.

Build the app around it exactly as you build the real one and hand it to the framework's
`TestApp` harness. Each publish through the harness drives the handlers to quiescence, so an
assertion never races them.

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

A `&[T]` handler mounts on the test broker unchanged: the transport assembles
[batches](queues.md#batches) on the client exactly as the real subscriber does, and
`assert_batch_sizes(..)` reports the batches the body was handed.

A handler reading AMQP fields through [`AmqpContext`](queues.md#delivery-metadata) mounts
unchanged too. The transport fills those fields from its own model: the default exchange, the
queue name as the routing key, a delivery tag numbered per subscription, and `redelivered` set on
a requeue.

## What the test broker does not simulate

Exchange routing, bindings, dead-lettering, prefetch and request/reply are server behaviour:
exercise them against a real RabbitMQ. The crate's own integration tests run that way, gated on
`AMQP_TEST_URL`:

```text
just brokers-up
AMQP_TEST_URL=amqp://127.0.0.1:5672 cargo test --workspace --all-features -- --test-threads=1
```
