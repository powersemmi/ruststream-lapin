# Queues and topology

A descriptor names the queue a handler consumes and describes what that queue is expected to look
like: durability, exchange bindings, prefetch, and raw `x-*` arguments. `RabbitQueue` is the
classic queue; a descriptor sits directly in the `#[subscriber(..)]` attribute:

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

RabbitMQ picks the queue implementation at declaration time via `x-queue-type`, and the type of an
existing queue can never change. So the implementation is the descriptor rather than a setting on
one: `RabbitQueue` is the classic single-node queue, `RabbitQuorumQueue` the Raft-replicated one.
Both are declared with the type they name.

What they describe reads the same - bindings, prefetch, dead-letter arguments, raw `x-*`
arguments, `.delay(..)` - and what differs is the retries, which [Capping the
retries](#capping-the-retries) covers.

A quorum queue is durable, shared and permanent by definition, so `.durable(..)`, `.exclusive(..)`
and `.auto_delete(..)` are not on it: a queue that contradicts its own type is not expressible
rather than refused when the service starts.

RabbitMQ 4 denies transient (non-durable) non-exclusive queues by default: on a classic queue keep
the durable default unless the queue is `.exclusive(true)`.

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
native dead-letter target, on either descriptor. A handler that drops a message settles with
`basic.reject(requeue = false)`, which routes it there. This is the queue's own topology, and it
applies to every rejection, whoever caused it.

## Capping the retries

A handler that keeps asking for another delivery circulates its message until an operator steps
in. Two steps after `include` end that:

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_topology.rs:declaration"
```

`max_attempts(n)` is how many deliveries one message gets, counting the first; `dead_letter(name)`
is where it goes once they run out, republished as it arrived. A destination is a routing key on
the default exchange here, so the name is the queue the spent delivery lands in.

On a quorum queue this service declares, the pair becomes the queue's own topology: it is declared
with `x-delivery-limit` and a dead-letter route to that name, and the server applies them. The
argument is one less than the cap, because the server counts the returns a message survives where
the cap counts the deliveries it gets. A message whose consumers keep dying then leaves the queue
with no service running to count it - which is also why `out_retry(policy)` does not compile on a
quorum queue: there is no copy of this service's to publish, so there is no publisher to name.

Declare both steps or neither. A cap with nowhere to send the spent delivery would drop it, so
subscribing refuses half a declaration and names the missing step.

A quorum queue this service does not declare carries the arguments whoever declared it gave it, so
subscribing refuses the pair there too rather than promising a cap nothing applies: set
`x-delivery-limit` and `x-dead-letter-exchange` on the broker and mount the handler plainly. The
crate reads the queue's count either way. A descriptor states the same policy where the queue is
the service's but the retries are not one handler's business:

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_topology.rs:quorum_own_policy"
```

A classic queue counts nothing and has no delivery limit, so there the cap is the runtime's: it
counts the attempts in the `x-ruststream-retry-count` header of the copies it publishes, and a
copy goes back under the queue's own name, which is what addresses the queue on the default
exchange. The copies leave through a publisher every registration already has, the broker's
default publish policy. Name another one where that will not do:

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_topology.rs:retry_mount"
```

The position takes the slot steps: `.codec(..)` and `.transform(..)` after it, and `.to(name)`
where the copy belongs somewhere other than the queue it came from. A transform there reads the
delivery being retried, the way a reply's transform reads the request.

A bare queue name takes the runtime's cap too. `#[subscriber("orders")]` carries no arguments to
declare a queue with, so the two steps stay what the runtime applies: the header counts the
deliveries and the spent one goes to the dead-letter name. That holds whatever the queue on the
broker turns out to be, so a quorum queue that is to carry a spent delivery away itself gets
`x-delivery-limit` and `x-dead-letter-exchange` where it is declared - from `RabbitQuorumQueue` in
a service that owns the queue, on the broker in a service that does not.

A handler's `retry()` settles with `basic.reject`, and RabbitMQ counts a rejected delivery as one
the message has spent - it counts `basic.nack` not at all, which is why this crate never sends
one. So a handler-driven loop runs a quorum queue's delivery limit down, and a delivery reports
how far the message has come through `redelivery_count()`. A classic queue keeps no such counter:
its `redelivered` flag says only that the message has been seen before, and every delivery off one
reports no count at all.

## Delayed retry

A handler that returns `HandlerOutcome::retry_after(delay)` asks for redelivery no sooner than
`delay`, the not-ready-yet case where an immediate requeue would spin. AMQP has no per-message
delay of its own, so the delayed copy is the runtime's to publish, on either queue type:

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_topology.rs:retry_fallback"
```

The copy waits in the service process, at-most-once over the window, and goes back under the
queue's own name.

`.delay(..)` puts the delay on the broker instead: the message parks in a waiting queue with a
per-message TTL and dead-letters back to the origin queue when the TTL fires, so a restart
mid-window loses nothing. The service publishes no copy of its own there.

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_topology.rs:delay"
```

The copy the waiting queue releases is a new message, and the server counts a new message from
zero: a quorum queue's own counter starts again on it. So the copy carries the framework's count
instead - the same `x-ruststream-retry-count` the runtime writes on the copies it publishes
itself, one higher every time it comes back. The runtime reads that count on this path as well, so
a cap declared over a queue with `.delay(..)` ends the loop the way it ends an immediate one: the
spent delivery goes to the dead-letter queue instead of waiting once more.

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
