---
sidebar_position: 3
title: bind_with
description: "bind_with binds a queue to a headers exchange by x-match and the headers to match, all of them or any one."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# bind_with

[`bind_with`](api:ruststream_lapin::RabbitQueue::bind_with) binds a queue to a [`headers`](api:ruststream_lapin::RabbitExchange::headers) exchange by `x-match` and the headers to match, all of them or any one.

A headers exchange ignores the routing key and routes on the message's headers.
`bind_with(exchange, routing_key, arguments)` gives the binding an argument table, a [`FieldTable`](api:ruststream_lapin::FieldTable) of [`AMQPValue`](api:ruststream_lapin::AMQPValue) entries.
The entry `x-match` decides the rule: `all` takes a message that carries every other entry of the table with the same value, and `any` takes a message that carries at least one.
The other entries are the headers to match, and the routing key of the binding is usually empty.
A plain `bind` to a headers exchange sends an empty table, and `all` of no headers is true of every message, so such a queue gets everything the exchange gets.

The publishing side sets the headers on the message, and they ride the AMQP header table the exchange reads.
`bind_with` also serves a [plugin exchange](./index.md) that routes on binding arguments.

## Example

The EU gold queue matches both headers, and the EU-or-gold queue matches either one.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/lapin_exchanges_headers.rs#headers"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/lapin_exchanges_headers.rs#headers"
```

</TabItem>
</Tabs>
