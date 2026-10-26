---
title: Settlement
description: "An ack sends basic.ack, a retry basic.reject with requeue, a drop basic.reject without it; RabbitMQ counts every rejection."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Settlement

A handler's answer settles its AMQP delivery with a protocol frame: [`ack`](api:ruststream::runtime::HandlerOutcome::ack) sends `basic.ack`, [`retry`](api:ruststream::runtime::HandlerOutcome::retry) sends `basic.reject` with requeue, and [`drop`](api:ruststream::runtime::HandlerOutcome::drop) sends `basic.reject` without it.

The frame goes out on the channel the delivery arrived on, and the service publishes nothing.
A requeued message goes back to its queue, which hands it out again with the `redelivered` flag set.
The next delivery may reach this consumer or another consumer of the same queue.
A handler reads the flag through the [`Redelivered`](api:ruststream_lapin::context::keys::Redelivered) key of the delivery context.
A dropped message leaves the queue: RabbitMQ discards it, or routes it to the queue's [dead-letter exchange](./dead-letter-exchange.md).

The crate settles with `basic.reject` and never with `basic.nack`.
On one delivery the two frames do the same thing, but RabbitMQ counts a rejected delivery as spent and does not count a nacked one.
So a handler that keeps asking for its message back runs down a quorum queue's [delivery limit](./delivery-limit.md).
A quorum queue stamps a returned message with the `x-delivery-count` header; a classic queue keeps no count, only the flag.

A retry under a [cap](./retries.md) is a requeue too: the quorum queue counts it.
A [delayed retry](./retry-after.md) travels as a copy through the queue's waiting queue.

After a connection loss, a delivery's old acknowledgement belongs to the old channel and can fail even if the connection has recovered.
RabbitMQ requeues unsettled messages from queues that survive the disconnect and may deliver them again.
Do not treat reconnection as proof that the old delivery was settled; handlers must tolerate redelivery when [recovery](../broker/connection.md#automatic-recovery) is enabled.

## Example

An order is requeued the first time it arrives and acknowledged when it comes back redelivered. An order without a SKU is dropped.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/lapin_settlement_outcomes.rs#outcomes"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/lapin_settlement_outcomes.rs#outcomes"
```

</TabItem>
</Tabs>
