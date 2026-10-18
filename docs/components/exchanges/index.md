---
title: RabbitExchange
description: "bind joins a queue to a RabbitExchange under a routing key; a direct exchange routes on the exact key, and the default exchange on queue names."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# RabbitExchange

[`bind`](api:ruststream_lapin::RabbitQueue::bind) joins a queue to a [`RabbitExchange`](api:ruststream_lapin::RabbitExchange) under a routing key; a direct exchange routes on the exact key, and the default exchange on queue names.

A publisher sends a message to an exchange, never to a queue.
The exchange copies the message into the queues bound to it whose bindings match.
`RabbitExchange` names the exchange of a binding, and its constructor names its type:

| Constructor | Routes on |
|---|---|
| [`direct`](api:ruststream_lapin::RabbitExchange::direct) | the exact routing key |
| [`topic`](./topic.md) | a dotted pattern of the routing key |
| [`fanout`](./fanout.md) | nothing: every bound queue gets every message |
| [`headers`](./bind-with.md) | the message's headers, against the binding's arguments |
| [`consistent_hash`](./consistent-hash.md) | a hash of the routing key, through a plugin |

`bind(exchange, routing_key)` adds a binding to a queue descriptor, and a queue may carry several.
A direct exchange delivers a message to every queue bound under its routing key exactly; a message under a key no queue is bound under goes nowhere.
The default exchange, with the empty name, is a direct exchange every queue is bound to under its own name.
So a message published with no exchange reaches the queue its routing key names, and a binding to the default exchange is refused.

An exchange is [`durable`](api:ruststream_lapin::RabbitExchange::durable) by default and survives a broker restart, and [`auto_delete`](api:ruststream_lapin::RabbitExchange::auto_delete) is off.
The exchange and its bindings reach the server when the broker [declares the topology](../broker/declare-topology.md); the built-in `amq.*` exchanges are bound to and never declared.
The publishing side names its exchange on the publish policy with [`exchange`](../publishing/exchange.md), and the message's name is its routing key.

[`RabbitExchange::custom(name, kind)`](api:ruststream_lapin::RabbitExchange::custom) names an exchange of a type a server plugin provides, such as `x-random` or `x-modulus-hash`.
A queue binds to it with `bind`, or with [`bind_with`](./bind-with.md) for a plugin that routes on binding arguments.
The plugin does the routing, so the server needs the plugin enabled, and the [in-process transport](../testing/index.md) routes nothing through such an exchange.

## Example

Checkout publishes each payment to the direct exchange `payments` under its method; the card queue is bound under `card`, the invoice queue under `invoice`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/lapin_exchanges_direct.rs#direct"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/lapin_exchanges_direct.rs#direct"
```

</TabItem>
</Tabs>
