---
sidebar_position: 4
title: consistent_hash
description: "consistent_hash spreads a stream over queues bound with integer weights, by a hash of the routing key, through the RabbitMQ plugin."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# consistent_hash

[`consistent_hash`](api:ruststream_lapin::RabbitExchange::consistent_hash) spreads a stream over queues bound with integer weights, by a hash of the routing key, through the RabbitMQ plugin.

The constructor exists with the crate's `plugin-consistent-hash` feature and names the `x-consistent-hash` type of the `rabbitmq_consistent_hash_exchange` plugin.
The server needs the plugin enabled: `rabbitmq-plugins enable rabbitmq_consistent_hash_exchange`.
Each queue binds with its weight as the routing key, such as `"1"` or `"2"`, and takes that share of the hash space.
The exchange hashes the routing key of each message, so messages with one key always reach the same queue.
So a key such as a customer or an account keeps its messages in one queue, in order, while the stream spreads over all of them.
Within one service instance, [lanes](../subscriptions/lanes.md) give the same per-key order without the plugin.

The plugin does the routing on the server.
The [in-process transport](../testing/index.md) declares the exchange and its bindings and routes nothing through it, so a test of the spread runs against a plugin-enabled server with [`TestApp::start_live`](../testing/live.md).

## Example

Orders are hashed by customer over two shards of equal weight.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/lapin_exchanges_consistent_hash.rs#shards"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/lapin_exchanges_consistent_hash.rs#shards"
```

</TabItem>
</Tabs>
