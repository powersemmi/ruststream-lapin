---
sidebar_position: 5
title: Prelude
description: "One glob, ruststream_lapin::prelude, brings the core prelude, the descriptors, the policies under the family's names and the context keys."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Prelude

One glob, [`ruststream_lapin::prelude`](api:ruststream_lapin::prelude), brings the core prelude, the descriptors, the policies under the family's names and the context keys.

`use ruststream_lapin::prelude::*;` is the one import a RabbitMQ routes file writes.
It carries the core's own prelude, so `#[subscriber]`, `RustStream`, `HandlerOutcome` and `Out` come with it.
It adds [`LapinBroker`](./index.md), the descriptors [`RabbitQueue`](../subscriptions/rabbit-queue.md) and [`RabbitQuorumQueue`](../subscriptions/rabbit-quorum-queue.md), [`RabbitExchange`](../exchanges/index.md), [`Delay`](../settlement/delay.md) and [`DirectReplyTo`](../request/direct-reply-to.md).
It adds the [context](../context/index.md) keys `Exchange`, `RoutingKey`, `Redelivered` and `DeliveryTag`, and the [argument](../subscriptions/arguments.md) types `AMQPValue` and `FieldTable`.
It also carries the transaction traits a handler settles a transaction with.

The publish policies come under the names every broker crate uses: `Publish` is [`LapinPublish`](../publishing/index.md), `TransactionalPublish` is [`ConfirmsPublish`](../transactions/confirms-publish.md), and `Request` is [`LapinRequest`](../request/index.md).
So a mount site reads the same on every broker.
[`ServerTxPublish`](../transactions/server-tx-publish.md) keeps its own name: an AMQP server transaction is a guarantee of its own, not a second spelling of `TransactionalPublish`.
Two broker preludes in one file collide on the uniform names; the prefixed types at the crate root tell them apart.

A handler body bounds its slots with capability traits, such as `Out<impl TransactionalPublisher>`, and names no broker type.
The one exception is a body that sets an AMQP property for a single message: it bounds its slot `Out<impl Publisher<Options = LapinPublishOptions>, Marker>` and calls the steps of [`LapinPublishSteps`](../publishing/properties/index.md), both from this prelude.

## Example

The routes file reads the routing key through a context key and replies through `TransactionalPublish`, all from the one glob.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/lapin_broker_prelude.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/lapin_broker_prelude.rs#app"
```

</TabItem>
</Tabs>
