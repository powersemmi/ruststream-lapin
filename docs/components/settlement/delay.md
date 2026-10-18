---
sidebar_position: 5
title: delay
description: "delay(Delay::dlx_ttl()) parks a delayed copy in a broker waiting queue under a TTL, which dead-letters it back when it fires."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# delay

[`delay(Delay::dlx_ttl())`](api:ruststream_lapin::RabbitQueue::delay) makes [`retry_after`](./retry-after.md) wait on the broker: the copy parks in a waiting queue under a per-message TTL.

On a `retry_after`, the crate publishes the copy to the waiting queue with the delay as its expiration, then acknowledges the original.
Both frames go out on one channel in that order, so a connection lost between them brings the original back rather than losing it.
The waiting queue has no consumers.
When the TTL fires, it dead-letters the copy back to the origin queue on the default exchange.
The copy lives on the broker, so it survives a restart of the service.
The waiting queue is `<queue>.retry`; [`Delay::dlx_ttl_named`](api:ruststream_lapin::Delay::dlx_ttl_named) names another.
The released copy is a new message to the server, and it carries `x-ruststream-retry-count` raised by one.
So a [retry cap](./retries.md) ends a delayed loop the way it ends an immediate one.

A waiting queue releases expired messages only from its head, so a short delay waits behind a long one.
Mixed delays take one waiting queue per delay class, or the [delayed-message plugin](./plugin-dme.md).

The waiting queue is infrastructure.
With [`declare_topology(true)`](api:ruststream_lapin::LapinBroker::declare_topology) the service declares it: durable, dead-lettering to the origin queue.
On a broker that declares nothing, the operator provisions it with those arguments.
The subscription checks at startup that the waiting queue exists, and the service fails to start without it.

## Example

A shipment that arrives while the warehouse is closed waits a minute in `shipments.retry`. The released copy carries the retry count, and the shipment goes out.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/lapin_settlement_delay.rs#delay"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/lapin_settlement_delay.rs#delay"
```

</TabItem>
</Tabs>
