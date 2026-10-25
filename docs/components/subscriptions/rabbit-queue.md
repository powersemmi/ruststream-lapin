---
sidebar_position: 1
title: RabbitQueue
description: "RabbitQueue consumes a classic queue: its durability, exclusivity, auto-delete, bindings and the settings shared with quorum queues."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# RabbitQueue

[`RabbitQueue`](api:ruststream_lapin::RabbitQueue) consumes a classic queue: its durability, exclusivity, auto-delete, bindings and the settings shared with quorum queues.

[`RabbitQueue::new`](api:ruststream_lapin::RabbitQueue::new) names the queue.
Three settings belong to the classic queue alone:

| Setting | Default | What it does |
|---|---|---|
| [`durable`](api:ruststream_lapin::RabbitQueue::durable) | `true` | the queue survives a broker restart |
| [`exclusive`](api:ruststream_lapin::RabbitQueue::exclusive) | `false` | the queue belongs to the connection that declared it and goes with it |
| [`auto_delete`](api:ruststream_lapin::RabbitQueue::auto_delete) | `false` | the server deletes the queue once its last consumer leaves |

RabbitMQ 4 refuses a transient queue that other connections may share, so `durable(false)` comes with `exclusive(true)`.
An exclusive queue suits a per-instance feed, such as a cache that every instance refreshes from a fanout exchange.

The other settings are shared with [`RabbitQuorumQueue`](./rabbit-quorum-queue.md):
[`bind`](../exchanges/index.md) and [`bind_with`](../exchanges/bind-with.md) join the queue to exchanges,
[`dead_letter_exchange`](../settlement/dead-letter-exchange.md) and `dead_letter_routing_key` give it a route for rejected messages,
[`prefetch`](./prefetch.md) and [`batch_wait`](./batches.md) shape its consumption,
[`delay`](../settlement/delay.md) parks delayed retries on the server,
and [`argument`](./arguments.md) passes anything else the queue declaration takes.
Bindings, the dead-letter route and the arguments reach the server when the broker [declares the topology](../broker/declare-topology.md).

A classic queue keeps no count of a message's deliveries.
A retry puts the message back on the queue, and its next delivery carries the `redelivered` flag, which a handler reads through the [`Redelivered`](../context/index.md) key.
Of the [retry steps](../settlement/retries.md) at the mount site, a classic queue takes `dead_letter(name)`, which becomes its dead-letter route.
It has no cap, so `max_attempts(n)` stops the service at startup; a quorum queue takes it.

## Example

The order queue is bound to a topic exchange and dead-letters to `orders.failed`; an order that fails once is placed on its redelivery.
The price queue belongs to this instance alone.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/lapin_subscriptions_rabbit_queue.rs#descriptor"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/lapin_subscriptions_rabbit_queue.rs#descriptor"
```

</TabItem>
</Tabs>
