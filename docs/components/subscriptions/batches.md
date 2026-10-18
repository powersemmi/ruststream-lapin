---
sidebar_position: 5
title: Batches
description: "A slice handler gets a batch assembled on the client, up to the mount site's size or until batch_wait (50 ms) passes since the first delivery."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Batches

A slice handler gets a batch assembled on the client, up to the mount site's size or until `batch_wait` (50 ms) passes since the first delivery.

A handler over `&[T]` is a batch handler, and [`batch(n)`](api:ruststream::runtime::SubscriberSettings::batch) at the mount site is its largest batch.
AMQP pushes one delivery at a time, so the subscription collects deliveries into a batch on the client.
A batch closes when it holds `n` deliveries, or when [`batch_wait`](api:ruststream_lapin::RabbitQueue::batch_wait) has passed since its first delivery, 50 ms by default.
A raised wait gives fuller batches on a sparse queue; a lowered one hands a short batch over sooner.
The server sends no more than the [prefetch](./prefetch.md) window, so a window below `n` keeps every batch below `n`.

One outcome settles every delivery of the batch.
In the manual form the body is an `impl Handle<[T]>`: `Ok(())` acknowledges the batch, and `Err(outcomes)` settles delivery `i` with outcome `i`.
`workers(n)` keeps up to `n` batches in flight at once.

## Example

Payments settle up to 32 at a time, and a batch that does not fill closes 200 ms after its first payment.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/lapin_subscriptions_batches.rs#batches"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/lapin_subscriptions_batches.rs#batches"
```

</TabItem>
</Tabs>
