---
sidebar_position: 3
description: "Route RabbitMQ messages through the default, topic, fanout and headers exchanges: bindings on the queue, the exchange on the publish policy."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Exchanges and routing

This guide routes messages through RabbitMQ exchanges: by routing key, to every bound queue, and by headers.

A publisher never writes to a queue.
It publishes to an exchange with a routing key, and the exchange copies the message into every queue whose binding matches.
In RustStream a queue's bindings are written on its [descriptor](../components/subscriptions/rabbit-queue.md), and the exchange a message goes to belongs to the [publish policy](../components/publishing/exchange.md).
The routing key is the message's name: the `#[outgoing(name = "..")]` of its type, or the name the publish gives it.

The example is a shop that announces orders on a topic exchange, price changes on a fanout exchange and shipments on a headers exchange.
The broker declares the exchanges, the queues and the bindings, so the example runs on an empty RabbitMQ ([Topology](topology.md)).

## The default exchange

Every service so far published to the default exchange.
It is a direct exchange that every queue is bound to under its own name.
So a message with the routing key `orders` reaches the queue `orders`, and a service reads it with `#[subscriber("orders")]` and no binding at all.
A binding on the default exchange is refused, by the server and by the in-process RabbitMQ alike.

A [direct exchange](../components/exchanges/index.md) of your own routes the same way: a binding names one routing key, and a message reaches the queues bound under exactly its key.

## Topic exchanges

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/routing.rs#topic"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/routing.rs#topic"
```

</TabItem>
</Tabs>

`place_order` publishes each order as `OrderPlaced` with the routing key `order.placed.<region>`.
The type declares no name, so the publish names it with `.to(&key)`.
A [topic exchange](../components/exchanges/topic.md) matches the key against each binding's pattern, word by word, with dots between the words.
`*` stands for exactly one word and `#` for any number of words, none included.

So `order.placed.*` takes the orders of every region, `*.*.eu` only the EU's, and `#` every message on the exchange.
An order from the US reaches the notifications and the audit, and the EU warehouse never sees it.
A handler reads the key a message arrived with through [`Ctx<RoutingKey>`](../components/context/index.md).

## Fanout exchanges

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/routing.rs#fanout"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/routing.rs#fanout"
```

</TabItem>
</Tabs>

A [fanout exchange](../components/exchanges/fanout.md) copies every message into every queue bound to it and ignores the routing key.
`change_price` replies with `PriceChanged`, and the storefront and the search index each get their own copy.
The bindings name an empty key, because the exchange reads none.
A new reader of price changes is one more queue bound to `prices`; the publisher does not change.

## Headers exchanges

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/routing.rs#headers"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/routing.rs#headers"
```

</TabItem>
</Tabs>

A headers exchange routes on the message's headers instead of the key.
[`bind_with`](../components/exchanges/bind-with.md) gives the binding its arguments: the headers to match and `x-match`, which says how many of them must match.
With `all`, a message needs every one of them; with `any`, one is enough.
`ship_order` sets `region` and `tier` on each shipment with `with_headers`.
A gold shipment in the EU reaches both queues, a silver one in the EU only the insurance, and a silver one in the US neither.

The plain `bind` is for exchanges that route on the key.
On a headers exchange it binds with no headers to match, and `all` of no headers is true of every message.
[`FieldTable`](api:ruststream_lapin::FieldTable) and [`AMQPValue`](api:ruststream_lapin::AMQPValue) come from lapin, re-exported in the prelude.

## Publishing to an exchange

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/routing.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/routing.rs#app"
```

</TabItem>
</Tabs>

The exchange is named once, on the policy at the mount site: [`Publish::default().exchange("events")`](api:ruststream_lapin::LapinPublish::exchange).
The handler's body names only the message and its key, so the same handler publishes to another exchange when the mount site says so.
A reply takes the same policy at `.out_reply(..)`.

RabbitMQ refuses a publish to an exchange that does not exist and closes the channel the publish went over.
Here the subscriptions declare `events`, `prices` and `shipments` at startup, before any handler publishes.
A service that publishes to an exchange it does not read from relies on operations or on another service to declare it.

Every binding of a subscription also appears in the service's AsyncAPI document, in the [`x-ruststream-amqp`](../components/asyncapi/extension.md) extension of its channel.

## Consistent hashing

The `plugin-consistent-hash` feature adds [`RabbitExchange::consistent_hash`](../components/exchanges/consistent-hash.md), the exchange type of RabbitMQ's consistent hash plugin.
It spreads one stream of messages over several queues by a hash of the routing key, and each queue binds with its weight as the routing key.
Messages with the same key always reach the same queue.
The plugin must be enabled on the server, and the routing happens only there, so its tests run against a live RabbitMQ.

## Testing

The tests in the example file publish an order, a price change and three paid orders, and check which queues each reached.
The in-process RabbitMQ routes through the default, direct, topic, fanout and headers exchanges by the same bindings the descriptors declare.
[Testing a RabbitMQ service](testing.md) covers the harness.
