---
sidebar_position: 1
title: exchange
description: "exchange sends a policy's publishes to a named exchange; without it the default exchange delivers to the queue named by the key."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# exchange

[`exchange`](api:ruststream_lapin::LapinPublish::exchange) sends a policy's publishes to a named exchange instead of the default exchange.

The message name stays the routing key, and the exchange routes by it.
A topic exchange matches it against binding patterns, a direct exchange matches it exactly, and a fanout exchange ignores it.
Without `exchange`, the default exchange delivers to the queue whose name equals the routing key.
RabbitMQ accepts a publish that matches no binding and drops it.
Every publish policy of the crate takes the setting: [`ConfirmsPublish`](api:ruststream_lapin::ConfirmsPublish), [`ServerTxPublish`](api:ruststream_lapin::ServerTxPublish) and [`LapinRequest`](api:ruststream_lapin::LapinRequest) too.
With [`declare_topology(true)`](api:ruststream_lapin::LapinBroker::declare_topology), the subscription that binds a queue to the exchange declares the exchange.

## Example

Every accepted order is announced on the `events` topic exchange under `order.placed`. An audit queue bound under `order.*` receives it.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/lapin_publishing_exchange.rs#exchange"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/lapin_publishing_exchange.rs#exchange"
```

</TabItem>
</Tabs>
