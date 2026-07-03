# Request/reply

RPC over RabbitMQ [direct reply-to](https://www.rabbitmq.com/docs/direct-reply-to), with both
sides expressed in the framework's ordinary forms: the requester is a capability handle from the
broker, and the responder is a plain publishing handler.

## The requester

`broker.requester()` implements the `RequestReply` capability. Every request goes out with
`reply-to` set to the `amq.rabbitmq.reply-to` pseudo-queue and a generated `correlation-id`;
replies come back on the requester's private consumer and are matched by correlation id:

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_request_reply.rs:request"
```

## The responder

The responder is an ordinary `#[subscriber(.., publish(..))]` handler: decode the request,
return the reply. `Err` settles without replying, and the requester's timeout is the recovery
mechanism:

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_request_reply.rs:handler"
```

What makes it an RPC responder is the reply destination, and that is publish-pipeline work, not
handler work. A static `PublishTransform` reads the incoming delivery through the
`PublishContext` and redirects each reply to the address the requester stamped on the request,
echoing its correlation id:

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_request_reply.rs:transform"
```

Compose it onto the reply publisher at mount time:

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_request_reply.rs:mount"
```

The handler stays a pure request-to-reply function, testable in-process like any other; the
direct reply-to convention lives in one reusable transform. The full runnable program is
[`examples/lapin_request_reply.rs`](https://github.com/powersemmi/ruststream-lapin/blob/main/crates/ruststream-lapin/examples/lapin_request_reply.rs).

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
