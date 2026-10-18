---
sidebar_position: 1
title: dead_letter_exchange
description: "dead_letter_exchange gives a queue its own route for every rejected or spent message; dead_letter_routing_key overrides the key."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# dead_letter_exchange

[`dead_letter_exchange`](api:ruststream_lapin::RabbitQueue::dead_letter_exchange) gives a queue its own route for the messages it rejects: the `x-dead-letter-exchange` argument.

A [drop](./index.md) settles with `basic.reject` without requeue, and the queue publishes the message to that exchange.
The route belongs to the queue, so it takes every message the queue dead-letters, whatever the cause.
Besides a rejection, RabbitMQ dead-letters a message whose TTL expired, one pushed out by a length limit, and one that spent a quorum queue's [delivery limit](./delivery-limit.md).
The message keeps its routing key, unless [`dead_letter_routing_key`](api:ruststream_lapin::RabbitQueue::dead_letter_routing_key) names another (`x-dead-letter-routing-key`).
The dead-letter exchange routes like any other exchange: a queue bound to it under that key collects the messages.
[`RabbitQuorumQueue`](api:ruststream_lapin::RabbitQuorumQueue) takes the same two settings.

Both settings are queue arguments.
The queue gets them when the service declares it, with [`declare_topology(true)`](api:ruststream_lapin::LapinBroker::declare_topology) on the broker.
A queue the operator provisions carries the arguments it was declared with.
The [`dead_letter(name)`](api:ruststream::runtime::Mounting::dead_letter) step of the mount chain is a different thing: the destination of a registration's [retry cap](./retries.md).

## Example

An invoice with a negative amount is dropped. The queue dead-letters it to `invoices.dlx`, where a second subscription reads it from the bound queue.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/lapin_settlement_dead_letter.rs#dlx"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/lapin_settlement_dead_letter.rs#dlx"
```

</TabItem>
</Tabs>
