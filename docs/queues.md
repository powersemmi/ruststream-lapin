# Queues and topology

A `RabbitQueue` descriptor names the queue a handler consumes and describes what that queue is
expected to look like: durability, queue type, exchange bindings, prefetch, and raw `x-*`
arguments. A descriptor sits directly in the `#[subscriber(..)]` attribute:

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_topology.rs:descriptor"
```

The bare-string form `#[subscriber("orders")]` is shorthand for `RabbitQueue::new("orders")`:
a durable, shared, non-auto-delete queue consumed as-is.

## Declaration is an opt-in

A descriptor states the topology a subscription expects. Nothing is created on the broker by
default, and a missing queue is a subscribe error: the infrastructure is yours to manage. A
service that owns its queues opts in per broker:

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_topology.rs:app"
```

With declaration enabled, subscribing declares the bound exchanges (except the built-in `amq.*`
ones and the default exchange), the queue, and the bindings, in that order. Declaration is
idempotent as long as the descriptor matches what exists; the broker rejects a redeclare with
different properties (`PRECONDITION_FAILED`).

## Queue types

RabbitMQ picks the queue implementation at declaration time via `x-queue-type`, and the type of
an existing queue can never change. The descriptor exposes it as a typed option:

- `.queue_type(QueueType::Classic)` - the classic single-node implementation.
- `.queue_type(QueueType::Quorum)` - Raft-replicated; must stay durable. The crate refuses to
  declare a non-durable quorum queue rather than letting the broker fail the declare.

A broker-wide `.default_queue_type(..)` applies to descriptors that do not pick a type; with
neither set, no `x-queue-type` is sent and the server default applies.

RabbitMQ 4 denies transient (non-durable) non-exclusive queues by default: keep the durable
default unless the queue is `.exclusive(true)`.

## Prefetch

`.prefetch(n)` on the broker sets the per-subscription `basic.qos` window: at most `n`
deliveries in flight unacknowledged. That is how back-pressure reaches the broker, since a slower
consumer slows the pushes instead of letting them buffer without bound. A descriptor overrides it
per queue with `.prefetch(n)`. Without either, the server imposes no limit.

The count is a `NonZeroU16`. AMQP reads `basic.qos(0)` as "no limit" rather than as a cap of
zero, so leaving the prefetch unset is how you ask for unlimited. Write the literal with the
framework's `nonzero!` macro, which rejects zero at compile time, as the descriptor above does.

## Batches

A handler taking a slice consumes a whole batch, and the mount site names how big a batch may be:

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_topology.rs:batches"
```

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_topology.rs:batches_mount"
```

AMQP has no wire-level batch - the broker pushes one `basic.deliver` at a time - so the batch is
assembled on the client: deliveries collect until the batch holds the size the mount asked for or
`.batch_wait(..)` elapses since the first of them, whichever comes first. Nothing at the mount
site says so: the size is all a batch registration names, and how the batch forms belongs to the
descriptor.

`.batch_wait(..)` defaults to 50 ms. Raise it on a slow link or a sparse queue where fuller
batches are worth the wait, and lower it where a batch arriving late costs more than a batch
arriving short.

A batch may be shorter than the size, because a partial batch goes to the handler rather than
waiting for traffic that may never come. And a batch holds only what the broker has already
pushed, so a [prefetch](#prefetch) window narrower than the batch size caps every batch at the
window: pair `.batch(n)` with a prefetch of at least `n`.

## Delivery metadata

`AmqpContext` carries the AMQP delivery metadata that is neither payload nor headers: the
exchange, the routing key, the redelivered flag, and the channel-local delivery tag. Each field
has a zero-sized key in `context::keys`, and a handler names the fields it needs as extractor
parameters, one key each:

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_keyed_lanes.rs:metadata"
```

A handler that declares `ctx: &mut Context<'_, AmqpContext>` reads the same fields with
`ctx.context(KEY)`. The prelude carries the keys but not the context type, so import that from
`ruststream_lapin::context`.

## Keyed worker lanes

A subscription can dispatch across several worker lanes with `workers(n, by_key)`, keeping
deliveries that share a key on the same lane (ordered per key, parallel across keys):

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_keyed_lanes.rs:consumer"
```

The producer sets the key in the `PARTITION_KEY_HEADER` (`amqp-partition-key`) and the crate
reads it back through the `Partitioned` capability. AMQP itself does not interpret the header, so
this is a client-side convention, unrelated to the server-side hash routing below.

## Dead-letter

`.dead_letter_exchange("dlx")` (plus optionally `.dead_letter_routing_key(..)`) sets the queue's
native dead-letter target. A handler that drops a message settles with
`basic.reject(requeue = false)`, which routes it there.

## Delayed retry

A handler that returns `HandlerOutcome::retry_after(delay)` asks for redelivery no sooner than
`delay`, the not-ready-yet case where an immediate requeue would spin. By default the runtime
handles this with its broker-agnostic fallback, and the delayed copy waits in the service process,
at-most-once over the window. `.delay(..)` makes it native instead: the message parks in a broker
waiting queue with a per-message TTL and dead-letters back to the origin queue when the TTL fires,
so the delayed copy lives on the broker.

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_topology.rs:delay"
```

The waiting queue (`<queue>.retry` by default, or `Delay::dlx_ttl_named(..)`) is infrastructure:
it is declared only under `declare_topology(true)`, otherwise provision it yourself. Because a
classic queue only releases expired messages from its head, use one waiting queue per delay class
(or a quorum queue) when delays vary widely.

For workloads with widely mixed delays, the `plugin-dme` feature offers `Delay::plugin_dme()`,
which routes redeliveries through the
[delayed-message-exchange](https://github.com/rabbitmq/rabbitmq-delayed-message-exchange) plugin
instead: the message carries an `x-delay` header and the plugin holds each one independently, so a
short delay never waits behind a long one. It needs the plugin enabled on the broker, hence the
feature gate.

## Consistent-hash exchange (plugin)

For server-side fan-out - spreading one stream across several queues by hashing the routing key -
the `plugin-consistent-hash` feature exposes `RabbitExchange::consistent_hash(..)`, lowering to the
[`rabbitmq_consistent_hash_exchange`](https://github.com/rabbitmq/rabbitmq-server/tree/main/deps/rabbitmq_consistent_hash_exchange)
plugin's exchange type. Each queue binds with its integer weight as the routing key, and the
broker splits the hash space proportionally:

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_consistent_hash.rs:shards"
```

Consistent-hash routing happens on the broker, across queues, where keyed worker lanes divide one
consumer's work across lanes. Enable the plugin on the broker before using it; the feature is off
by default because the plugin is not part of a stock RabbitMQ.

## Raw arguments

Anything the descriptor does not model is sent verbatim:

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_topology.rs:arguments"
```

`AMQPValue` and `FieldTable` are re-exported from the crate for exactly this purpose.
