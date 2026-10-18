---
sidebar_position: 2
title: RabbitQuorumQueue
description: "RabbitQuorumQueue consumes a replicated quorum queue, which is always durable and counts the deliveries each message spends."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# RabbitQuorumQueue

[`RabbitQuorumQueue`](api:ruststream_lapin::RabbitQuorumQueue) consumes a replicated quorum queue, which is always durable and counts the deliveries each message spends.

A quorum queue keeps its messages on several nodes through Raft, so it survives the loss of a node.
It is durable, shared and permanent by definition.
So the descriptor has no `durable`, `exclusive` or `auto_delete` step, and a quorum queue that contradicts its type does not compile.
Every [shared setting](./rabbit-queue.md) of `RabbitQueue` is on it too.

The server counts each delivery of a message, and a retry spends one.
A quorum delivery reports its count, where a classic delivery reports only the `redelivered` flag.
Past the queue's delivery limit the server takes the message off the queue: it dead-letters the message when the queue has a [dead-letter exchange](../settlement/dead-letter-exchange.md), and drops it otherwise.
RabbitMQ 4 gives a quorum queue a limit of 20 when it names none, so a message that keeps failing gets 21 deliveries.
[`delivery_limit`](../settlement/delivery-limit.md) sets another limit; `max_attempts` with `dead_letter` at the mount site sets it from the registration.

The queue type is part of the declaration, so a quorum queue is created by an operator or by a broker that [declares the topology](../broker/declare-topology.md).
A quorum queue refuses some classic arguments: a declaration with `x-max-priority` fails with [`AmqpError::Declare`](api:ruststream_lapin::AmqpError::Declare).

## Example

A reservation waits for stock; one that never fits ends after the queue's default delivery limit.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/lapin_subscriptions_quorum.rs#quorum"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/lapin_subscriptions_quorum.rs#quorum"
```

</TabItem>
</Tabs>
