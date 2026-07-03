# Request/reply

`broker.requester()` implements the framework's `RequestReply` capability over RabbitMQ
[direct reply-to](https://www.rabbitmq.com/docs/direct-reply-to): requests carry
`reply-to = amq.rabbitmq.reply-to` and a generated `correlation-id`; RabbitMQ rewrites the
address per request, and all replies arrive on one private consumer where the requester
multiplexes them back by correlation id.

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_request_reply.rs:request"
```

The responder needs no special machinery: it is an ordinary subscriber that publishes its reply
to the `reply-to` address it received (a plain publish to the default exchange), echoing the
`correlation-id` header. Both values arrive as regular headers on the request:

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_request_reply.rs:responder"
```

## Semantics

- **At-most-once.** Direct reply-to keeps reply state in the requester's channel on one broker
  node; nothing is queued durably. A dropped requester channel loses in-flight replies, and the
  per-request timeout is the recovery mechanism. An unanswered request fails with a timeout
  error.
- **Transient by default.** Requests are published with delivery mode 1: a request nobody is
  waiting for after the timeout gains nothing from surviving a broker restart. Opt into
  persistence with `.persistent(true)`.
- **No infrastructure.** The pseudo-queue is never declared; the only real entity involved is
  the request queue the responder consumes.
