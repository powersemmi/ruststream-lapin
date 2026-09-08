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

## The routes file does not change

The mount site is where a test usually starts diverging from production, and here it does not:
the crate's publish policies pair against the test broker too. Each one produces the stand-in for
the publisher it produces on a server, carrying the same capabilities:

| Mount site | On RabbitMQ | On `LapinTestBroker` | Capabilities |
| --- | --- | --- | --- |
| `Publish` | `LapinPublisher` | `LapinTestPublisher` | publish |
| `TransactionalPublish` | `ConfirmsPublisher` | `ConfirmsTestPublisher` | publish, borrowed and owned transactions |
| `ServerTxPublish` | `ServerTxPublisher` | `ServerTxTestPublisher` | publish, borrowed transactions |
| `Request` | `LapinRequester` | `LapinTestRequester` | publish, request/reply |

The parity cuts both ways, which is the point of spelling it out. A handler bounded
`Out<impl TransactionalPublisher>` mounted on `Publish` fails to compile against the test broker
exactly as it fails against a server, so a passing test never covers wiring that cannot ship.

Request/reply comes along with it. A handler that calls another service mid-flow
(`Out<impl RequestReply>`, mounted with `Request::default()`) runs in process against a responder
in the same app: the transport gives every request a private reply address and a correlation id,
the `DirectReplyTo` transform sends the reply back to it, and a reply that does not correlate is
dropped rather than resolving the call. An unanswered request fails with the same timeout error a
server produces, which is the branch a handler actually codes against.

[Batches](queues.md#batches) come along: the transport assembles them on the client exactly as the
real subscriber does, so a `&[T]` handler mounts on the test broker unchanged and
`assert_batch_sizes(..)` reports the batches the body was handed.

Delivery metadata comes along too: a handler reading AMQP fields through
[`AmqpContext`](queues.md#delivery-metadata) - as `Ctx<RoutingKey>` extractors or a
`ctx: &mut Context<'_, AmqpContext>` parameter - mounts on the test broker unchanged. The
transport reports those fields against its own model: the default exchange, the queue name as the
routing key, a delivery tag numbered per subscription, and `redelivered` set on a requeue.

A queue with several consumers behaves like the work queue it is: each delivery goes to exactly
one of them, in rotation, and a `nack(requeue = true)` goes back through the queue rather than to
the consumer that rejected it. Scaling a handler horizontally is therefore testable in process -
two subscriptions on one queue share the work instead of each seeing a copy.

## What the test broker does not simulate

The transport models routing and consumers, not storage: a queue exists only as the set of
subscriptions listening on its name, and a message lives in a consumer's channel. Everything
below follows from that one line, and every item is exercised against a real server by the
crate's integration tests instead.

- **Nothing waits for a consumer.** A message published to a queue nobody consumes is dropped,
  the way an unroutable message is, and a subscription only ever sees what was published after it
  opened. This is the contract the framework's own conformance suite requires of an in-process
  broker, not an omission.
- **Prefetch.** With no backlog to hold messages in, there is nothing to withhold: deliveries are
  pushed as they arrive, no consumer is ever skipped for being at its unacked limit, and
  `prefetch(..)` changes no distribution. Prefetch bounds are a server behaviour.
- **Dead-lettering.** A dropped message ends there. A dead-letter exchange resolves through the
  binding table, which is the other half of what the transport does not model.
- **Exchanges and bindings.** Routing is by exact queue name, so a policy's `exchange(..)` is
  accepted and ignored. Exchange types, bindings, and the alternate-exchange path need a server.
- **Publisher confirms.** `TransactionalPublish` reproduces the client-side buffer exactly -
  buffer, replay in order on commit, discard on abort - but nothing in process can nack a message,
  so a successful commit is not evidence that a broker accepted the batch.
- **AMQP server transactions.** `ServerTxPublish` reproduces the visibility boundary by holding
  the messages in the client instead of on the broker, so a test proves when they become visible
  and nothing about atomicity or about what a lost connection does to a transaction in flight.
- **Direct reply-to's at-most-once nature.** Reply addresses are subscriptions here, not channel
  state on one broker node, so replies cannot be lost with a connection.

```text
just brokers-up
AMQP_TEST_URL=amqp://127.0.0.1:5672 cargo test --workspace --all-features -- --test-threads=1
```
