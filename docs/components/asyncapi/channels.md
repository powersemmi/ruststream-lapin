---
sidebar_position: 2
title: Channels
description: "A subscription's channel is its queue with durability and exclusivity; a publish names a routing key on an exchange, or a queue."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Channels

A subscription's channel is its queue with durability and exclusivity; a publish names a routing key on an exchange, or a queue.

A subscription's channel binding is `is: queue`.
Its `queue` object carries the name and the `durable`, `exclusive` and `autoDelete` settings of the [`RabbitQueue`](../subscriptions/rabbit-queue.md) descriptor.

A channel the service publishes to is described from its publish policy.
Its name is the destination the mount site resolved: a reply's name, the name of a slot entry, or a `dead_letter(..)` declaration.
On a policy with a named [`exchange`](../publishing/exchange.md), the name is the routing key: the binding is `is: routingKey` with the exchange's name.
On the default exchange the routing key is the queue a message lands in: the binding is `is: queue` with that queue's name.
A queue's settings belong to the side that declares it, so this binding carries the name alone.

A reply under [`DirectReplyTo`](../request/direct-reply-to.md) goes to the address each request carries.
Its channel therefore has no `address`, and the receive [operation](./operations.md) names the header the address is read from.

## Example

The channel `inventory.check` reports a transient, exclusive, auto-delete queue.
The reply channel `inventory.unrouted` reports the exchange `answers`.
The dead-letter channel `inventory.dead` reports its address alone: the server moves a dead letter, and no publish of the service's leads there.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/lapin_asyncapi.rs#channels"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/lapin_asyncapi.rs#channels"
```

</TabItem>
</Tabs>
