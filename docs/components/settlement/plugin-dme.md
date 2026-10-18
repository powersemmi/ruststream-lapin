---
sidebar_position: 6
title: plugin_dme
description: "Delay::plugin_dme waits in the delayed-message-exchange plugin instead, so a short delay never queues behind a long one."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# plugin_dme

[`Delay::plugin_dme`](api:ruststream_lapin::Delay::plugin_dme) hands a delayed copy to RabbitMQ's delayed-message-exchange plugin instead of a waiting queue.

The crate publishes the copy to an `x-delayed-message` exchange with the delay in its `x-delay` header, then acknowledges the original.
The plugin holds each message for its own delay and then routes it to the origin queue.
So a short delay never waits behind a long one, and one exchange serves every delay.
The exchange is `<queue>.delay`; [`Delay::plugin_dme_named`](api:ruststream_lapin::Delay::plugin_dme_named) names another, which several queues may share.
The released copy carries `x-ruststream-retry-count` raised by one, as with a [waiting queue](./delay.md).

The backend sits behind the crate's `plugin-dme` feature, and the plugin has to be enabled on the server.
With [`declare_topology(true)`](api:ruststream_lapin::LapinBroker::declare_topology) the service declares the exchange and binds the origin queue to it.
On a broker that declares nothing, the operator provisions both, and the subscription checks at startup that the exchange exists.

## Example

Each reminder waits as long as it asks for. The in-process transport releases the copy like a waiting queue; the plugin's own release order shows only against a server with the plugin.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/lapin_settlement_plugin_dme.rs#dme"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/lapin_settlement_plugin_dme.rs#dme"
```

</TabItem>
</Tabs>
