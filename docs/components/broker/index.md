---
title: LapinBroker
description: "LapinBroker connects a service to RabbitMQ; new only records the URI, and the runtime connects it at startup and shuts it down at the end."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# LapinBroker

[`LapinBroker`](api:ruststream_lapin::LapinBroker) connects a service to RabbitMQ; `new` only records the URI, and the runtime connects it at startup and shuts it down at the end.

[`LapinBroker::new`](api:ruststream_lapin::LapinBroker::new) takes the [connection URI](./connection.md).
It does no I/O, so the whole service fits the synchronous `#[ruststream::app]` builder.
The settings that follow it are builder steps too: a [connection name](./connection.md), a default [prefetch](../subscriptions/prefetch.md) window and [`declare_topology`](./declare-topology.md).

At startup the runtime connects the broker and receives a [`ConnectedLapinBroker`](api:ruststream_lapin::ConnectedLapinBroker).
The connect opens the AMQP connection and logs in, so an unreachable server or a refused login fails startup instead of the first delivery.
Every subscription and every publisher of the service opens against that connected form.
At [shutdown](./shutdown.md) the runtime closes the connection and receives a [`ClosedLapinBroker`](api:ruststream_lapin::ClosedLapinBroker).

`#[subscriber("orders")]` consumes the queue `orders`.
The queue has to exist on the server, unless the broker [declares the topology](./declare-topology.md).
The descriptors that bind a queue to an exchange or choose its type are on the [Subscriptions](../subscriptions/index.md) pages.

## Example

The service consumes the queue `orders` on a server at `localhost:5672`.
`cargo run --example lapin_broker_service --features testing -- run` starts it.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/lapin_broker_service.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/lapin_broker_service.rs#app"
```

</TabItem>
</Tabs>
