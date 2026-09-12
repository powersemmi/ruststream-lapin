# Publishing

`LapinPublish` is the policy that constructs this broker's publisher, `LapinPublisher`. You set
the options on the policy where the broker is named, and the runtime instantiates the publisher on
the connected broker at startup.

The message name is the routing key. The exchange is a property of the policy: the default
exchange unless `.exchange("events")` names another. On the default exchange the routing key
addresses the queue with that name, which is why the quickstart works with no topology at all.

Four headers map onto native AMQP properties: `content-type`, `correlation-id`, `reply-to` and
`message-id`. Every other header goes into the AMQP header table as a byte string, so binary values
round-trip.

## Per-message AMQP properties

Three AMQP properties belong to the message rather than to the publisher: the `priority`, the
per-message `expiration` (TTL), and the delivery mode. The mount site fixes what every message
carries, and the publish builder adjusts one of them for one message.

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_priority.rs:mount"
```

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_priority.rs:steps"
```

The three steps are `priority(n)`, `expiration(ttl)` and `persistent(flag)`, and the policy carries
the same three names for the defaults. What a publish leaves alone is what the mount site fixed.

- `priority` orders deliveries on a queue declared with `x-max-priority`; on any other queue it
  reaches the consumer without affecting the order. Unset by default.
- `expiration` makes the broker drop the message once the TTL passes without it being consumed,
  dead-lettering it when the queue says so. Unset by default, so messages do not expire.
- `persistent` is delivery mode 2, which is the default. Opt out where losing messages on a broker
  restart is acceptable.

A step is a position on the publish builder, not a wrapper around the publisher. So a stepped
publish keeps the codec and the destination the mount site gave it, it holds through both
transaction kinds, and under `TestApp` it stays attributed to its `Out` slot. A handler body that
takes a step is the one place a body names this crate: bound the slot
`Out<impl Publisher<Options = LapinPublishOptions>, Marker>` and import
`ruststream_lapin::prelude::*`.

None of the three travels as a header. Written into the header table - under the protocol's own
names (`priority`, `expiration`) or under the names a delivery reports them with - a value reaches
RabbitMQ as a table entry it reads for no purpose, and the message is delivered without the
property.

A delivery does report them back as headers, under `amqp-priority` and `amqp-expiration`
(`PRIORITY_HEADER` / `EXPIRATION_HEADER`), so a handler reads an incoming priority where it reads
everything else.

## Replying from a handler

A publishing handler returns the reply value, and the runtime publishes it. The reply type
declares where it goes with `#[outgoing(name = "..")]`. A type that declares none is published
under the name the `publish("..")` clause gives.

Either name is a routing key, and the policy at the mount site names the exchange:
`.out(Reply, Publish::default())` replies on the default exchange, and
`.out(Reply, Publish::default().exchange("events"))` on a topic exchange. The steps after it fill
the rest of the reply wiring: `.codec(..)` sets the reply codec, `.transform(..)` changes each
reply before it is published.

The [core publishing guide](https://powersemmi.github.io/ruststream/) has the whole reply surface.
The [request/reply page](request-reply.md) shows the RPC variant, where a transform redirects each
reply to the requester's private address.

## Three publishers

`LapinPublish::default()` is fire-and-forget: the publish resolves when the frame is written,
with no broker feedback. The publishing mode is a policy transition, so a stronger guarantee is a
different type:

- `.confirms()` - `ConfirmsPublish`, publisher confirms: a publish resolves only once the broker
  confirmed it. Transactions buffer client-side and flush on commit. Durable and fast; the
  recommended transactional publisher.
- `.server_tx()` - `ServerTxPublish`, AMQP channel transactions (`tx.select` / `tx.commit` /
  `tx.rollback`): messages become visible atomically at commit. It costs a synchronous round trip
  per commit, and it is the only option when a partial flush is unacceptable.

A routes file writes the two it reaches for under the family's uniform mount-site names, which
the [prelude](index.md) aliases: `Publish` is `LapinPublish` and `TransactionalPublish` is
`ConfirmsPublish`, so a router reads the same whichever broker it is written against.
`ServerTxPublish` keeps its own name: it is a different guarantee, not a second spelling.

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_transactions.rs:confirms"
```

The trade-off: confirms give per-message durability, where a failed commit may leave earlier
messages published; server transactions give all-or-nothing visibility.

## Transactional fan-out from a handler

Bind the policy to the handler's slot at the mount site (`.out(marker, policy)`, sealed with
`.build()`), and the handler takes the live publisher as an `Out` parameter. Here an order fans
out into per-item shipment commands, published all-or-nothing:

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_transactions.rs:dispatch"
```

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_transactions.rs:handler"
```

Both transactional publishers implement the framework's `TransactionalPublisher`, so either one
plugs into the same `begin_transaction / commit / abort` call sites. A call that is invalid in the
current state returns an error instead of passing silently: a commit or abort with no open
transaction, and a second begin while one is open, which leaves the open transaction intact. A
publisher is a reference-counted handle, so every clone works with one channel and one transaction
state.

## Owned or borrowed transactions

The framework has two transaction shapes, and which ones a publisher offers follows the
transport:

- **Borrowed** - the handle carries the transaction. `begin()` gives a scope over it, or call
  `begin_transaction / commit / abort` on the raw publisher. Exactly one can be open per handle,
  so a second begin returns an error. Both publishers support this.
- **Owned** - the transaction is a value that owns its buffer, opened by `owned_transaction()`
  (or `OwnedTransactions::transaction` on the raw publisher).
  Any number can be open on one handle at a time, settling one never touches another, and the
  handle keeps publishing directly meanwhile. `commit` and `abort` consume the value, so a
  double commit or a publish after settling is a compile error. Only the confirms publisher
  supports this: its transaction is a client-side buffer, while `server_tx` puts the channel
  itself into transactional mode, which is channel state with exactly one instance.

Use the owned kind when one handler drives several independent groups of messages, and the
borrowed one when a whole scope of code publishes into one shared transaction. On a failed commit
the owned transaction is consumed and its buffer is lost: redelivery of the inputs, not
resubmission of the buffer, is the recovery path.
