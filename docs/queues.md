# Queues and topology

A `RabbitQueue` descriptor names the queue a handler consumes and describes what that queue is
expected to look like: durability, queue type, exchange bindings, prefetch, and raw `x-*`
arguments. A descriptor sits directly in the `#[subscriber(...)]` decorator:

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_topology.rs:descriptor"
```

The bare-string form `#[subscriber("orders")]` is shorthand for `RabbitQueue::new("orders")`:
a durable, shared, non-auto-delete queue consumed as-is.

## Declaration is an opt-in

Descriptors describe the EXPECTED topology. By default nothing is created on the broker: a
missing queue is a subscribe error, because managing infrastructure is the user's job, not the
framework's. A service that owns its queues opts in per broker:

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_topology.rs:app"
```

With declaration enabled, subscribing declares the bound exchanges (except the built-in `amq.*`
ones and the default exchange), the queue, and the bindings, in that order. Declaration is
idempotent as long as the descriptor matches what exists; AMQP refuses to redeclare an entity
with different properties (`PRECONDITION_FAILED`).

## Queue types

RabbitMQ picks the queue implementation at declaration time via `x-queue-type`, and the type of
an existing queue can never change. The descriptor exposes it as a typed option:

- `.queue_type(QueueType::Classic)` - the classic single-node implementation.
- `.queue_type(QueueType::Quorum)` - Raft-replicated; must stay durable (the crate rejects a
  quorum descriptor marked non-durable instead of letting the broker fail the declare).

A broker-wide `.default_queue_type(..)` applies to descriptors that do not pick a type; with
neither set, no `x-queue-type` is sent and the server default applies.

Note that RabbitMQ 4 denies transient (non-durable) non-exclusive queues by default: keep the
durable default unless the queue is `.exclusive(true)`.

## Prefetch

`.prefetch(n)` on the broker sets the per-subscription `basic.qos` window: at most `n`
deliveries in flight unacknowledged. This is the back-pressure valve for the subscriber stream -
consuming slower slows the broker's pushes instead of buffering without bound. A descriptor
overrides it per queue with `.prefetch(n)`. Without either, the server imposes no limit.

## Dead-letter

`.dead_letter_exchange("dlx")` (plus optionally `.dead_letter_routing_key(..)`) sets the queue's
native dead-letter target. A handler that drops a message settles with
`basic.reject(requeue = false)`, which routes it there; no extra machinery is involved.

## Delayed retry

A handler that returns `HandlerResult::retry_after(delay)` asks for redelivery no sooner than
`delay` - the not-ready-yet case, where an immediate requeue would just spin. By default the
runtime handles this with its broker-agnostic fallback (the delayed copy waits in the service
process, at-most-once over the window). `.delay(..)` makes it native instead: the message parks
in a broker waiting queue with a per-message TTL and dead-letters back to the origin queue when
the TTL fires, so the delayed copy lives on the broker.

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_topology.rs:delay"
```

The waiting queue (`<queue>.retry` by default, or `Delay::dlx_ttl_named(..)`) is infrastructure:
it is declared only under `declare_topology(true)`, otherwise provision it yourself. Because a
classic queue only releases expired messages from its head, use one waiting queue per delay class
(or a quorum queue) when delays vary widely.

## Raw arguments

Anything the descriptor does not model rides through verbatim:

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_topology.rs:arguments"
```

`AMQPValue` and `FieldTable` are re-exported from the crate for exactly this purpose.
