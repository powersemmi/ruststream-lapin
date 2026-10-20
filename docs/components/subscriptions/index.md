---
title: Subscriptions
description: "A queue name in #[subscriber] consumes that existing queue as a classic queue; a descriptor carries every other setting."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Subscriptions

A queue name in `#[subscriber]` consumes that existing queue as a classic queue; a descriptor carries every other setting.

One subscription consumes one queue.
`#[subscriber("orders")]` consumes the queue `orders` with the defaults of [`RabbitQueue`](./rabbit-queue.md): a durable classic queue, the broker's [prefetch](./prefetch.md) and no bindings.
The queue has to exist on the server, unless the broker [declares the topology](../broker/declare-topology.md).
A message reaches it through the default exchange, which routes on the queue's name, or through an [exchange](../exchanges/index.md) the queue is bound to.

RabbitMQ has two queue types, and the type is fixed when the queue is created.
The type decides what a retry does, so each type has a descriptor of its own:

| Descriptor | Queue | A retry is |
|---|---|---|
| [`RabbitQueue`](./rabbit-queue.md) | classic, on one node | counted by the runtime, on the copies it publishes back |
| [`RabbitQuorumQueue`](./rabbit-quorum-queue.md) | quorum, replicated | counted by the server, which ends a message that spent its deliveries |

A descriptor goes where the name goes, as the attribute's first argument, and every one of its steps returns the descriptor.
The manual form passes the name or the descriptor to [`subscriber(source, body)`](api:ruststream::runtime::subscriber).
Several instances of a service share one queue, and each message goes to one of them.
The handler's outcome is a protocol frame: an ack removes the message, and a retry or a drop rejects it, as the [Settlement](../settlement/index.md) pages show.

With [automatic recovery](../broker/connection.md#automatic-recovery) enabled, a subscription waits while Lapin restores its channel and consumer, then resumes through the same handle.
Unsettled deliveries may be redelivered after an outage, so handlers must tolerate duplicates.
When retries run out, the stream yields a consume error and ends. Recovery is disabled by default.

## Example

An order with items is acknowledged; an empty order is dropped.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/lapin_subscriptions_queue.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/lapin_subscriptions_queue.rs#handler"
```

</TabItem>
</Tabs>
