---
title: AmqpContext
description: "Ctx keys give a handler the exchange, routing key, redelivered flag and delivery tag; Context<'_, AmqpContext> reads them all."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# AmqpContext

[`Ctx`](api:ruststream::runtime::Ctx) keys give a handler the exchange, routing key, redelivered flag and delivery tag; `Context<'_, AmqpContext>` reads them all.

[`AmqpContext`](api:ruststream_lapin::context::AmqpContext) carries what a delivery has beside its payload and its headers.
The subscription builds it once per delivery.
A handler binds one field with `Ctx<K>`, where `K` is a key of [`context::keys`](api:ruststream_lapin::context::keys):

| Key | Value | What it is |
| --- | --- | --- |
| [`Exchange`](api:ruststream_lapin::context::keys::Exchange) | `String` | the exchange the message was published to, empty for the default exchange |
| [`RoutingKey`](api:ruststream_lapin::context::keys::RoutingKey) | `String` | the routing key the message was published with |
| [`Redelivered`](api:ruststream_lapin::context::keys::Redelivered) | `bool` | whether the server handed the message out before |
| [`DeliveryTag`](api:ruststream_lapin::context::keys::DeliveryTag) | `u64` | the delivery's number on its channel |

The four keys are in the prelude.
A body that reads several fields takes `ctx: &mut Context<'_, AmqpContext>` instead and calls `ctx.context(key)` per field, as the manual form does; there the exchange and the routing key are borrowed `&str`.
`AmqpContext` itself comes from `ruststream_lapin::context`.

The routing key tells apart the messages a [topic](../exchanges/topic.md) binding takes.
`Redelivered` is set on a delivery a classic queue hands out again after a [retry](../settlement/index.md); a quorum queue counts deliveries instead.
The delivery tag numbers the deliveries of one channel, so it identifies a delivery only within its subscription.

## Example

The handler replies with where each order came from.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/lapin_context.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/lapin_context.rs#handler"
```

</TabItem>
</Tabs>
