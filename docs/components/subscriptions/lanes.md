---
sidebar_position: 6
title: Lanes
description: "workers(n, by_key) keeps deliveries that share the amqp-partition-key header on one lane, in order, while other keys run in parallel."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Lanes

`workers(n, by_key)` keeps deliveries that share the `amqp-partition-key` header on one lane, in order, while other keys run in parallel.

`workers(n, by_key)` runs `n` sequential lanes over one queue.
The lane of a delivery is picked by a hash of its partition key, so deliveries with one key run one after another, in queue order.
Deliveries with different keys run at once on different lanes.
A delivery without a key goes to the next lane in rotation.

On RabbitMQ the key is the header [`PARTITION_KEY_HEADER`](api:ruststream_lapin::PARTITION_KEY_HEADER), `amqp-partition-key`.
AMQP gives the header no meaning: it is a convention between the publisher, which sets it, and this crate, which reads it back.
A service that publishes to a keyed queue sets the header on each message, such as the tenant or the account the message belongs to.
The lanes order work inside one service instance.
Spreading a stream over several queues by key is the [consistent-hash exchange](../exchanges/consistent-hash.md), which the server runs.
The manual form writes the lanes as `.workers_by_key(n)` on the mount chain.

## Example

Orders of one tenant run in order on one lane, and different tenants run side by side.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/lapin_subscriptions_lanes.rs#lanes"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/lapin_subscriptions_lanes.rs#lanes"
```

</TabItem>
</Tabs>
