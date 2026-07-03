# Testing

The `testing` feature ships `LapinTestBroker`, an in-process transport that stands in for
RabbitMQ in application tests: the same handlers, descriptors, and wiring, no server. It routes
by exact queue name (the default-exchange model), records every publish, and implements
`ruststream::testing::TestableBroker`, so the framework's `TestApp` harness can drive it to
quiescence deterministically.

```toml
[dev-dependencies]
ruststream-lapin = { version = "0.5", features = ["testing"] }
```

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_testing.rs:handler"
```

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_testing.rs:testapp"
```

## What the test broker does not simulate

Exchange types, bindings, dead-lettering, prefetch, and request/reply are transport behavior;
exercise them against a real server. The crate's own integration tests run that way, gated on
`AMQP_TEST_URL`:

```text
just brokers-up
AMQP_TEST_URL=amqp://127.0.0.1:5672 cargo test --workspace --all-features -- --test-threads=1
```

## The conformance contract

The in-process transport is not a hand-rolled fake with private semantics: it passes the
framework's broker conformance suite (ordering, settlement, headers, publish log), and the real
broker passes the lifecycle and capability suites in CI. Broker authors extending this crate can
hold their changes to the same bar:

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_testing.rs:conformance"
```
