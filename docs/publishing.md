# Publishing

A publisher is declared as a policy and comes alive against the connection. `LapinPublish` holds
the options only, so it is constructible anywhere - in a router definition, at a mount site, in
configuration - and the runtime pairs it with the connected broker at startup. There is no
publisher without a connection to publish through.

The message name is the routing key. The exchange is a property of the policy: the default
exchange unless `.exchange("events")` says otherwise. On the default exchange the routing key
addresses the queue with that name, which is why the quickstart works with no topology at all.

Messages are published persistent (delivery mode 2) by default; `.persistent(false)` opts out
for fire-and-forget traffic where losing messages on a broker restart is acceptable.

Well-known headers map onto native AMQP properties (`content-type`, `correlation-id`,
`reply-to`, `message-id`); every other header travels in the AMQP header table as a byte string,
so binary values round-trip.

## Per-message AMQP properties

Core 0.7 unified publishing behind one builder: `message(..)` for a value, `raw(..)` for bytes,
then `to(..)`, `with_headers(..)`, and `publish()`. The builder is broker-agnostic, so the AMQP
values that are neither payload nor header have no position in it. This crate adds them in front
of it, as steps on the publisher itself:

- `with_priority(n)` - the AMQP `priority` property. It orders deliveries on a queue declared
  with `x-max-priority`; elsewhere the broker just carries it to the consumer.
- `with_expiration(ttl)` - the per-message `expiration` (TTL). The broker drops the message once
  the TTL passes without it being consumed, dead-lettering it when the queue says so.

Both come from `LapinPublishExt`, both hand back a publisher the builder continues on, and they
chain. A handler reaches them by bounding its slot with the trait, and takes the step on the
publisher it is injected:

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_priority.rs:steps"
```

Headers named `priority` or `expiration` do **not** do this. Every header that is not one of the
four well-known names travels in the AMQP header table, and RabbitMQ reads neither of these two
from there, so writing one is a request the broker silently ignores. That is what the steps are
for.

Two boundaries are worth knowing. A publish that took a step is visible in the broker's publish
log but is not attributed to the slot under `TestApp` (the slot's capture sits on the core
publish path), and the in-process test broker has no AMQP frame to carry a property at all - so
assert on properties against a real broker. Inside a transaction the steps work on the borrowed
form: `begin_transaction` / `commit` around a stepped publish keeps the properties through the
buffer. An owned transaction takes the message as the core trait hands it over and has no
property position, so no step goes in front of `transaction()`.

## Replying from a handler

The framework's `publish(..)` form works unchanged: the handler returns the reply value and the
runtime encodes and publishes it through the `TypedPublisher` the mount was given (see the
[core publishing guide](https://powersemmi.github.io/ruststream/) for the whole surface,
including per-publisher transforms and app-wide publish layers). The
[request/reply page](request-reply.md) shows the RPC variant, where a transform redirects each
reply to the requester's private address.

## Three publishers

`LapinPublish::default()` is fire-and-forget: the publish resolves when the frame is written,
with no broker feedback. The publishing mode is a policy transition, so picking a stronger
guarantee changes the type:

- `.confirms()` - `ConfirmsPublish`, publisher confirms: every publish resolves only once the
  broker confirmed it. Transactions buffer client-side and flush on commit. Durable and fast;
  the recommended transactional publisher.
- `.server_tx()` - `ServerTxPublish`, AMQP channel transactions (`tx.select` / `tx.commit` /
  `tx.rollback`): messages become visible atomically at commit. Slower (a synchronous round trip
  per commit), but the only option when partial flushes are unacceptable.

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_transactions.rs:confirms"
```

The trade-off: confirms give per-message durability (a failed commit may leave earlier messages
published), server transactions give all-or-nothing visibility.

## Transactional fan-out from a handler

Attach the policy at the mount site and the handler receives the live publisher as an `Out`
parameter. Here an order fans out into per-item shipment commands, published all-or-nothing:

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_transactions.rs:dispatch"
```

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_transactions.rs:handler"
```

Both transactional publishers implement the framework's `TransactionalPublisher`, so either
plugs into the same `begin_transaction / commit / abort` call sites. A call that is invalid in
the current state errors instead of passing silently: a commit or abort with no open
transaction, and a second begin while one is open (which leaves the open transaction intact).
Clones of a publisher share the underlying channel and transaction state.

## Owned or borrowed transactions

The framework has two transaction shapes, and which ones a publisher offers follows the
transport:

- **Borrowed** - the handle carries the transaction. `TypedPublisher::transactional()` then
  `begin()` gives a scope over it, or call `begin_transaction / commit / abort` on the raw
  publisher. Exactly one can be open per handle, so a second begin errors. Both publishers
  support this.
- **Owned** - the transaction is a value that owns its buffer, opened by
  `TypedPublisher::transaction()` (or `OwnedTransactions::transaction` on the raw publisher).
  Any number can be open on one handle at a time, settling one never touches another, and the
  handle keeps publishing directly meanwhile. `commit` and `abort` consume the value, so a
  double commit or a publish after settling is a compile error. Only the confirms publisher
  supports this: its transaction is a client-side buffer, while `server_tx` puts the channel
  itself into transactional mode, which is channel state with exactly one instance.

Use the owned kind when one handler drives several independent groups of messages; use the
borrowed one when a whole scope of code should publish into one shared transaction. On a failed
commit the owned transaction is consumed and its buffer is lost - redelivery of the inputs, not
resubmission of the buffer, is the recovery path.
