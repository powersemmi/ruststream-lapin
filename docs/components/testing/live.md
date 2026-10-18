---
sidebar_position: 1
title: Live
description: "TestApp::start_live runs the same test body against a running RabbitMQ, one subscription per queue, for what only a server has."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Live

[`TestApp::start_live`](api:ruststream::testing::TestApp::start_live) runs the same test body against a running RabbitMQ, one subscription per queue, for what only a server has.

It connects the broker through its ordinary connect, so every subscription consumes from the server.
The body is the one an in-process test runs; only the start call differs.
A publish, `settle` and `advance` wait on the real clock until every subscription has handled what was published to it.
The wait lasts ten seconds at most, and [`start_live_within`](api:ruststream::testing::TestApp::start_live_within) sets another bound.

The live mode is for what the in-process transport leaves to the server: the [prefetch](../subscriptions/prefetch.md) window, publisher confirms, the atomicity of a server transaction, a plugin exchange's routing, and a topology the service finds without declaring it.
A live test keeps one subscription per queue.
The server picks which consumer of a queue takes a message, so a test with two subscriptions of one queue stops at the first publish and names the queue.

A server keeps what earlier runs left in it.
In the example the service declares its queue with [`declare_topology`](../broker/declare-topology.md), and the queue is exclusive: it lives as long as the test's connection.

`live::url` is the repository's test helper: it returns the server's address from `AMQP_TEST_URL`, and the test returns early without one, so `cargo test` passes on a machine with no server.
A job that stood a server up sets `RUSTSTREAM_REQUIRE_LIVE=1`, and a missing address then fails the test instead of skipping it.
The repository's stand is `docker-compose.test.yml`, which `just brokers-up` starts.

## Example

The two tests share one body: one in process, one against the server at `AMQP_TEST_URL`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/lapin_testing_live.rs#start-live"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/lapin_testing_live.rs#start-live"
```

</TabItem>
</Tabs>
