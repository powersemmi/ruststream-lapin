---
sidebar_position: 3
title: delivery_limit
description: "A quorum queue ends a poison loop itself: delivery_limit, or max_attempts with dead_letter, becomes its x-delivery-limit."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# delivery_limit

[`delivery_limit(n)`](api:ruststream_lapin::RabbitQuorumQueue::delivery_limit) sets a quorum queue's `x-delivery-limit`: how many times the queue returns a message before it carries it away.

The argument counts returns, so the queue delivers a message `n + 1` times: `delivery_limit(4)` delivers it five times.
A quorum queue counts a delivery the handler rejected and a delivery whose consumer went away without settling it.
Once the count passes the limit, the queue sends the message to its [dead-letter exchange](./dead-letter-exchange.md), or drops it when it has none.
The server applies the limit itself: it holds while no service is running, and the service publishes nothing.
A quorum queue without a limit of its own takes RabbitMQ's default of 20 returns.
A returned message carries the queue's count in the `x-delivery-count` header.

A registration states the same through the mount chain: [`max_attempts(n)`](api:ruststream::runtime::Mounting::max_attempts) and [`dead_letter(name)`](api:ruststream::runtime::Mounting::dead_letter).
On a quorum queue the pair becomes an `x-delivery-limit` of `n - 1` and a dead-letter route to the queue `name`, written over the descriptor's limit.
The pair reaches the queue only when the service declares it, so the broker sets [`declare_topology(true)`](api:ruststream_lapin::LapinBroker::declare_topology).
On a broker that declares nothing the subscription refuses to start, and so does half of the pair.
[`out_retry(policy)`](api:ruststream::runtime::Mounting::out_retry) does not compile on a quorum queue, because no copy leaves this process.

## Example

The provider refuses a charge every time. The queue delivers it five times and then moves it to `charges.dlx`, where a classic queue keeps it.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/lapin_settlement_delivery_limit.rs#limit"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/lapin_settlement_delivery_limit.rs#limit"
```

</TabItem>
</Tabs>
