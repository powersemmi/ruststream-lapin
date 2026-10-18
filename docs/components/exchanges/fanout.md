---
sidebar_position: 2
title: fanout
description: "A fanout exchange copies every message to every bound queue and ignores the routing key."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# fanout

A [`fanout`](api:ruststream_lapin::RabbitExchange::fanout) exchange copies every message to every bound queue and ignores the routing key.

A queue binds to a fanout exchange with any routing key, usually the empty one.
Each bound queue gets its own copy, and each copy is consumed and settled on its own.
So a fanout exchange is the broadcast of RabbitMQ: one event, many independent consumers.
A service that needs a copy in every instance binds an [exclusive queue](../subscriptions/rabbit-queue.md) per instance.

## Example

A price change reaches both the search index and the cache, each through a queue of its own.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/lapin_exchanges_fanout.rs#fanout"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/lapin_exchanges_fanout.rs#fanout"
```

</TabItem>
</Tabs>
