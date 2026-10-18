---
sidebar_position: 1
title: topic
description: "A topic exchange routes on dotted routing-key patterns, where * matches one word and # any number of words."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# topic

A [`topic`](api:ruststream_lapin::RabbitExchange::topic) exchange routes on dotted routing-key patterns, where `*` matches one word and `#` any number of words.

A routing key of a topic exchange is a list of words separated by dots, such as `order.created` or `order.shipped.late`.
A queue binds with a pattern of the same shape.
`*` stands for exactly one word, and `#` for zero or more words.
So `order.*` takes `order.created` and passes `order.shipped.late` by, and `order.#` takes both.
A pattern without wildcards matches its key exactly, as on a direct exchange.
A message that matches several bindings of one queue reaches that queue once.

A handler that serves several keys reads the key of each delivery through the [`RoutingKey`](../context/index.md) context key.

## Example

Order steps go to the topic exchange `events` as `order.<step>`; the audit queue takes every one-word step, and the shipping queue takes `order.shipped` alone.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/lapin_exchanges_topic.rs#topic"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/lapin_exchanges_topic.rs#topic"
```

</TabItem>
</Tabs>
