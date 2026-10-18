---
sidebar_position: 4
title: Extension
description: "The x-ruststream-amqp extension lists each binding of a subscription: exchange, type, routing key and arguments, in declaration order."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Extension

The `x-ruststream-amqp` extension lists each binding of a subscription: exchange, type, routing key and arguments, in declaration order.

The specification's channel binding is a queue or a routing key, and it has no field for what joins the two.
So a subscription reports its bindings in this crate's extension, beside the `amqp` binding of its channel.
The extension holds a `bindings` list with one entry per [`bind`](../exchanges/index.md) and [`bind_with`](../exchanges/bind-with.md), in the order the descriptor declares them.

An entry carries the `exchange` name and its `type`: `direct`, `fanout`, `headers`, `topic`, or the type a plugin exchange declares.
The `routingKey` is written even when it is empty, because an empty key is the binding key of a fanout or a headers exchange.
The `arguments` appear when the binding has any, each value in the JSON shape of its AMQP type.
A subscription with no binding has no extension.
An extension is not a binding, so it carries no `bindingVersion`.

## Example

The queue `inventory.events` is bound to a topic exchange by its key and to a headers exchange by its arguments.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/lapin_asyncapi.rs#extension"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/lapin_asyncapi.rs#extension"
```

</TabItem>
</Tabs>

The service's document, in excerpt: the extension sits under the channel `inventory.events`.
A test holds the service to every value of the excerpt.

```json reference="tests/snapshots/asyncapi-amqp.json"
```
