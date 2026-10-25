---
sidebar_position: 8
description: "Test a RabbitMQ service in process with TestApp: the production app, outcomes, replies, delays, and the same test against a running RabbitMQ."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Testing a RabbitMQ service

A test runs the service's own app with [`TestApp`](api:ruststream::testing::TestApp), against an [in-process RabbitMQ](../components/testing/index.md) or against a running server.

The service under test charges card payments.
A payment arrives on the `card-payments` queue, and the handler replies with a receipt on `card-receipts`.
A payment without an amount is dropped, and the queue moves it to `card-payments.review`.
A payment over the bank's instant limit is held: the handler asks for it again in a minute with [`retry_after`](../components/settlement/retry-after.md).
The minute passes in the queue's waiting queue, `card-payments.retry`.
The app is built by `service(broker)`, so a test chooses the broker and `main` uses the production one.
The tests need the `testing` feature of `ruststream-lapin` in `[dev-dependencies]`.

## A payment and its receipt

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/testing.rs#in_process"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#in_process"
```

</TabItem>
</Tabs>

[`TestApp::start`](api:ruststream::testing::TestApp::start) connects [`LapinBroker`](../components/broker/index.md) to an in-process RabbitMQ.
The URI is parsed as a connection would parse it, and never dialled.
The test addresses the broker by its production type, `tb.broker::<LapinBroker>()`.
`Payment` declares its destination, `card-payments`, so the test publishes it with no `to(..)`.
`publish` returns once the handler and its reply have settled.
So the next lines assert on finished state: how the subscriber settled and what reached `card-receipts`.
The body takes the started app as an argument, so the live test below runs it too.

## A dropped payment

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/testing.rs#dropped"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#dropped"
```

</TabItem>
</Tabs>

`settled` checks the outcome the handler returned.
`assert_not_called` checks that no receipt was published.

## A delay, without waiting for it

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/testing.rs#held"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#held"
```

</TabItem>
</Tabs>

The test starts on a paused clock, and [`advance`](api:ruststream::testing::TestApp::advance) moves it forward.
A minute of `HOLD` costs nothing.
The queue has a [`delay`](../components/settlement/delay.md), so the in-process RabbitMQ holds the copy in the waiting queue and releases it to `card-payments` when the delay ends.
The copy is the broker's, so `published` shows the test's own message alone.
Each held delivery is recorded as `Outcome::Nack`.

## What the in-process RabbitMQ models

The in-process RabbitMQ reads the broker's settings and the descriptors of the routes, the same ones the server sees.
[`declare_topology`](../components/broker/declare-topology.md) decides whether a subscription declares its queue, as on the server.
It models RabbitMQ rather than a generic queue:

- The default, direct, topic, fanout and headers exchanges route by the bindings the descriptors declare.
- Competing consumers share a queue, and a requeue goes back through it.
- A queue keeps what reaches it while nobody consumes it, and a consumer that closes returns what it took and did not settle.
- A dropped message leaves through its queue's dead-letter route, and a quorum queue counts deliveries and dead-letters a spent one the same way.
- A waiting queue releases a delayed copy, and direct reply-to gives every request an address of its own.
- A transaction stays invisible until it commits.

It refuses what the server refuses: a name or a header the protocol cannot carry, a body over 16 MiB, a transient queue that is not exclusive, a binding on the default exchange, a queue declared again with other settings, a quorum queue with a priority.
A delivery settled after its connection closed reports the closed channel.

What only a server has needs one: the prefetch window, publisher confirms, the atomicity of a channel transaction, plugin exchanges, a queue two connections share, and a queue the service expects to find without declaring it.

## The same test on a server

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/testing.rs#live"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#live"
```

</TabItem>
</Tabs>

[`TestApp::start_live`](../components/testing/live.md) connects the same app to a running RabbitMQ.
The test reads the URI from `AMQP_TEST_URL` with `live::url`, a helper of the example's own, and skips when it is unset.
The broker declares its topology, so the test runs on an empty server.
The assertions are the in-process ones, unchanged.
On a server, RabbitMQ picks which consumer of a queue takes a message, so a live test runs one subscription per queue.

```bash
cargo test
AMQP_TEST_URL=amqp://127.0.0.1:5672 cargo test -- --test-threads=1
```

The RabbitMQ of [Getting started](getting-started.md) is enough for the live run.
