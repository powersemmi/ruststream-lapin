---
sidebar_position: 6
description: "Publish to RabbitMQ with the guarantee a message needs: fire-and-forget, publisher confirms, channel transactions, and per-message priority and TTL."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Reliable publishing

This guide chooses how sure a service is that RabbitMQ took its messages, and sets the properties of one message.

The guarantee is a property of the [publish policy](../components/publishing/index.md) at the mount site, not a flag on each call:

| Policy | Prelude name | A publish resolves when |
|---|---|---|
| [`LapinPublish`](api:ruststream_lapin::LapinPublish) | `Publish` | the frame is written to the connection |
| [`ConfirmsPublish`](../components/transactions/confirms-publish.md) | `TransactionalPublish` | RabbitMQ confirmed the message |
| [`ServerTxPublish`](../components/transactions/server-tx-publish.md) | `ServerTxPublish` | the channel transaction commits |

The example is an orders service.
It turns an order into one shipment per parcel, all of them or none, and books a courier for each shipment, an expedited one ahead of the rest.

## Confirmed transactions

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/publishing.rs#confirms"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/publishing.rs#confirms"
```

</TabItem>
</Tabs>

`pack_order` bounds its slot `Out<impl TransactionalPublisher>`, so it mounts only on a policy with transactions.
`begin` opens a scope, and every shipment published in it waits in the service.
`commit` publishes them and resolves once RabbitMQ has confirmed each one.
A parcel without a description aborts the scope, and no shipment of that order leaves.
A commit that fails returns an error, and the order can be delivered again and packed again.

RabbitMQ confirms a message once it has taken responsibility for it: written to disk for a persistent message on a durable queue, accepted by a quorum queue's majority.
RabbitMQ's [confirms guide](https://www.rabbitmq.com/docs/confirms) describes when each message is confirmed.

A confirm does not mean a queue received the message.
RabbitMQ confirms a message that matches no binding and drops it, because the crate publishes without the `mandatory` flag.
So a confirmed publish goes to an exchange whose bindings exist, or to a queue that exists: declare them with the service ([Topology](topology.md)), or check them where operations own them.

## Channel transactions

[`ServerTxPublish`](../components/transactions/server-tx-publish.md), reached with `Publish::default().server_tx()`, uses AMQP channel transactions instead.
The messages of a transaction become visible together at `commit`, which is a synchronous round trip to the server.
Confirms are faster and cover most cases.
Take a channel transaction when the consumers must never see half of a group, for example both legs of a transfer.

A handler bounded `Out<impl TransactionalPublisher>` mounts on either policy unchanged.
A [`ConfirmsTransaction`](../components/transactions/confirms-transaction.md), opened with `owned_transaction`, owns its buffer instead of borrowing the publisher: several can be open at once, and each commits on its own.
Channel transactions offer only the borrowed scope, because the transaction is the state of the channel itself.

## Per-message properties

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/publishing.rs#properties"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/publishing.rs#properties"
```

</TabItem>
</Tabs>

AMQP gives each message three properties of its own: the [priority](../components/publishing/properties/priority.md), the [expiration](../components/publishing/properties/expiration.md) and the [delivery mode](../components/publishing/properties/persistent.md).
The policy sets what every message through it carries, here priority 3.
The publish builder's [steps](../components/publishing/properties/index.md) set them for one message: an expedited pickup goes out at priority 9 and expires after an hour unread.
The slot is bounded on [`LapinPublishOptions`](api:ruststream_lapin::LapinPublishOptions), so the handler names no publisher type; the steps come from the crate's prelude.

None of the three travels as a header: each is an AMQP property, where RabbitMQ reads it.
A delivery reports them back as headers, under [`PRIORITY_HEADER`](api:ruststream_lapin::PRIORITY_HEADER) and [`EXPIRATION_HEADER`](api:ruststream_lapin::EXPIRATION_HEADER).

A priority orders deliveries only on a queue declared with `x-max-priority`; on any other queue RabbitMQ ignores it.
The couriers' `pickups` queue is declared with that [argument](../components/subscriptions/arguments.md) by whoever owns it.
Every policy publishes persistent messages unless `persistent(false)` says otherwise, so a message on a durable queue survives a broker restart.

## The mount site

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/publishing.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/publishing.rs#app"
```

</TabItem>
</Tabs>

`TransactionalPublish::default()` is [`ConfirmsPublish`](api:ruststream_lapin::ConfirmsPublish).
`Publish::default().priority(3)` is the plain policy with a priority for every message.
`.confirms()` and `.server_tx()` move a plain policy to the stronger ones and keep its settings, such as its exchange.
Fire-and-forget publishes share one channel of the connection; a confirms or transactional publisher opens a channel of its own, because both modes are channel state.

A service publishes outside a handler too, for example from an HTTP endpoint, through a publisher the app hands out at startup ([`LapinPublish`](../components/publishing/index.md)).

## Testing

The tests in the example file check that an order ships every parcel, that a bad parcel ships none, and which properties each pickup carries, read with `tb.out::<Pickups>()`.
The in-process RabbitMQ buffers a transaction until it commits and frames each message as a server would.
What a confirm means and the atomicity of a channel transaction are the server's, so they belong to a [live test](../components/testing/live.md).
[Testing a RabbitMQ service](testing.md) covers the harness.
