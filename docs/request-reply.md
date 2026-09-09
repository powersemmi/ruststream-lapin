# Request/reply

RPC over RabbitMQ [direct reply-to](https://www.rabbitmq.com/docs/direct-reply-to) is how one
service asks another a question through the broker it already has, instead of growing an HTTP
sidechannel: an order service checks stock in the inventory service before it accepts an order.
The two halves are two ordinary services; the runnable pair
is [`lapin_rpc_server`](https://github.com/powersemmi/ruststream-lapin/blob/main/crates/ruststream-lapin/examples/lapin_rpc_server.rs)
and [`lapin_rpc_client`](https://github.com/powersemmi/ruststream-lapin/blob/main/crates/ruststream-lapin/examples/lapin_rpc_client.rs).

## The requester

`LapinRequest` is the policy that constructs the requester `LapinRequester`, which implements the
`RequestReply` capability. Every request goes out with `reply-to` set to the direct reply-to
pseudo-queue and a generated `correlation-id`, and the matching reply resolves the call. Wrap the
capability in a small typed call:

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_rpc_client.rs:client"
```

Bind the policy to the handler's slot at the mount site
(`b.include(handler).out(DefaultSlot, Request::default()).build()`)
and the handler takes the live requester as an `Out` parameter, so it calls the other service in
the middle of its own message flow. The RPC timeout is the failure boundary: a business answer
settles the message, an unreachable service asks for redelivery:

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_rpc_client.rs:handler"
```

## The responder

The responder is an ordinary publishing handler: decode the request, return the reply. `Err`
settles without replying, and the requester's timeout is the recovery mechanism.

An RPC reply goes wherever the request asked, so its type declares no destination of its own. The
`publish("..")` clause names the address for a request that arrives without a reply-to header:

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_rpc_server.rs:handler"
```

What makes it an RPC responder is the transform. `DirectReplyTo` sends each reply back to the
address the requester asked for and echoes its correlation id; compose it onto the reply publisher
at mount time:

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_rpc_server.rs:mount"
```

The handler stays a pure request-to-reply function, testable in-process like any other.

## Semantics

- **At-most-once.** Direct reply-to keeps reply state in the requester's channel on one broker
  node, and nothing is queued durably. A dropped channel loses the replies in flight, and an
  unanswered request returns a timeout error.
- **Transient by default.** Requests are published with delivery mode 1, while an ordinary
  publish is persistent. You can mark them persistent with `.persistent(true)` on the policy.
- **No infrastructure.** The pseudo-queue is never declared, so the only real entity involved is
  the request queue the responder consumes. A responder on another stack interoperates as long as
  it publishes the reply to the received `reply-to` and echoes the `correlation-id`.
