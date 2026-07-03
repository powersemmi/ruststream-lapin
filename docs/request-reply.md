# Request/reply

RPC over RabbitMQ [direct reply-to](https://www.rabbitmq.com/docs/direct-reply-to) is how one
service asks another a question through the broker it already has, instead of growing an HTTP
sidechannel: an order service checks stock in the inventory service, a gateway fetches a price,
a saga step confirms a reservation. The two halves are two ordinary services; the runnable pair
is [`lapin_rpc_server`](https://github.com/powersemmi/ruststream-lapin/blob/main/crates/ruststream-lapin/examples/lapin_rpc_server.rs)
and [`lapin_rpc_client`](https://github.com/powersemmi/ruststream-lapin/blob/main/crates/ruststream-lapin/examples/lapin_rpc_client.rs).

## The requester

`broker.requester()` implements the `RequestReply` capability: every request goes out with
`reply-to` set to the direct reply-to pseudo-queue and a generated `correlation-id`, and the
matching reply resolves the call. Wrap the raw capability in a small typed client and put it in
the application state, like any other shared dependency:

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_rpc_client.rs:client"
```

Handlers then request it by type and call the other service in the middle of their own message
flow. The RPC timeout is the failure boundary, and it maps straight onto settlement: a business
answer settles the order, an unreachable service asks for redelivery:

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_rpc_client.rs:handler"
```

## The responder

The responder is an ordinary `#[subscriber(.., publish(..))]` handler: decode the request,
return the reply. `Err` settles without replying, and the requester's timeout is the recovery
mechanism:

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_rpc_server.rs:handler"
```

What makes it an RPC responder is the reply destination. `DirectReplyTo` is a ready-made publish
transform that sends each reply back to the address the requester asked for and echoes its
correlation id; compose it onto the reply publisher at mount time:

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_rpc_server.rs:mount"
```

The handler stays a pure request-to-reply function, testable in-process like any other.

## Semantics

- **At-most-once.** Direct reply-to keeps reply state in the requester's channel on one broker
  node; nothing is queued durably. A dropped requester channel loses in-flight replies, and the
  per-request timeout is the recovery mechanism. An unanswered request fails with a timeout
  error.
- **Transient by default.** Requests are published with delivery mode 1: a request nobody is
  waiting for after the timeout gains nothing from surviving a broker restart. Opt into
  persistence with `.persistent(true)` on the requester.
- **No infrastructure.** The pseudo-queue is never declared; the only real entity involved is
  the request queue the responder consumes. Responders on other stacks interoperate as long as
  they publish the reply to the received `reply-to` and echo `correlation-id`.
