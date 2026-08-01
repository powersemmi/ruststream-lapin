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

The trade-off in one sentence: confirms give per-message durability (a failed commit may leave
earlier messages published), server transactions give all-or-nothing visibility.

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
plugs into the same `begin_transaction / commit / abort` call sites. A call that makes no sense
in the current state errors instead of passing silently: a commit or abort with no open
transaction, and a second begin while one is open (which leaves the open transaction intact).
Clones of a publisher share the underlying channel and transaction state.
