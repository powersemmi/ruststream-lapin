---
sidebar_position: 3
title: prefetch
description: "prefetch caps unacknowledged deliveries per subscription on the broker or one descriptor; unset, the server applies no limit."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# prefetch

`prefetch` caps unacknowledged deliveries per subscription on the broker or one descriptor; unset, the server applies no limit.

The server pushes deliveries to a consumer without waiting to be asked.
The prefetch window (`basic.qos`) is how many deliveries it may have out to one subscription before the service settles some of them.
So the window is the back-pressure between a queue and a service: a slow handler holds the rest of the queue on the server, where other instances can take it.

[`LapinBroker::prefetch`](api:ruststream_lapin::LapinBroker::prefetch) sets the window for every subscription of the broker.
[`RabbitQueue::prefetch`](api:ruststream_lapin::RabbitQueue::prefetch) and [`RabbitQuorumQueue::prefetch`](api:ruststream_lapin::RabbitQuorumQueue::prefetch) set it for one subscription and take precedence.
The count is a `NonZeroU16`, because AMQP reads a window of 0 as no limit.
A subscription with neither setting has no window, and the server pushes it the whole queue.
A [batch handler](./batches.md) needs a window of at least its batch size, or its batches stay short.

The window is the server's: the [in-process transport](../testing/index.md) has none and hands a subscription everything its queue holds.
A test of the window runs against a server with [`TestApp::start_live`](../testing/live.md).

## Example

Every subscription holds up to 64 deliveries, and the slow report queue holds one.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/lapin_subscriptions_prefetch.rs#prefetch"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/lapin_subscriptions_prefetch.rs#prefetch"
```

</TabItem>
</Tabs>
