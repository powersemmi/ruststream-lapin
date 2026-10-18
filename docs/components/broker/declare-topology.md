---
sidebar_position: 3
title: declare_topology
description: "declare_topology(true) declares the exchanges, the queue and the bindings a descriptor states; off, the topology is the operator's and must exist."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# declare_topology

[`declare_topology(true)`](api:ruststream_lapin::LapinBroker::declare_topology) declares the exchanges, the queue and the bindings a descriptor states; off, the topology is the operator's and must exist.

The setting is off by default.
A descriptor then states the topology the service expects, and the service creates nothing.
A subscription to a queue the server does not have fails startup with [`AmqpError::Subscribe`](api:ruststream_lapin::AmqpError::Subscribe).

With the setting on, each subscription declares its topology before it starts consuming.
It declares the exchanges its bindings name, then the queue, then the bindings, in that order.
The built-in `amq.*` exchanges and the default exchange are never declared: the server already has them.
A declaration repeats safely while the descriptor matches what the server holds.
A queue or an exchange declared again with other settings is refused by the server with `PRECONDITION_FAILED`, and startup fails with [`AmqpError::Declare`](api:ruststream_lapin::AmqpError::Declare).
RabbitMQ 4 refuses a transient queue that is not exclusive, so a [`RabbitQueue`](../subscriptions/rabbit-queue.md) declared `durable(false)` has to be `exclusive(true)` too.

Some settings exist only as declaration arguments, so they reach the server only from a service that declares.
A [quorum queue's delivery limit](../settlement/delivery-limit.md) and the waiting queue of a [delayed redelivery](../settlement/delay.md) are two of them.

The [in-process transport](../testing/index.md) finds every queue a subscription names, so a test shows no missing queue.
A declaration the server refuses is refused in process too.

## Example

The service declares the topic exchange `events`, the queue `orders` and the binding `order.*` between them.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/lapin_broker_declare_topology.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/lapin_broker_declare_topology.rs#app"
```

</TabItem>
</Tabs>
