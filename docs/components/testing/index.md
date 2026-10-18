---
title: Testing
description: "TestApp::start runs the production app on LapinBroker in process, with exchanges, bindings, requeues, quorum counts and reply-to."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Testing

[`TestApp::start`](api:ruststream::testing::TestApp::start) runs the production app on [`LapinBroker`](../broker/index.md) in process, with exchanges, bindings, requeues, quorum counts and reply-to.

A test hands the harness the app `main` runs, unchanged, and addresses the broker by its production type, `tb.broker::<LapinBroker>()`.
The `testing` feature in `[dev-dependencies]` adds the in-process mode, and turns on the core's harness with it.

The in-process transport has no settings of its own.
It reads the broker's: [`declare_topology`](../broker/declare-topology.md) decides whether a subscription declares its queue, as on the server.
A publish is framed by the same conversion a publish to the server goes through.

The transport keeps RabbitMQ's semantics.
The default exchange and the direct, topic, fanout and headers exchanges route through the bindings the descriptors declare.
Consumers of one queue compete for its messages, and a requeue puts a message back in its queue.
A queue keeps what reaches it from its first subscription on, until an auto-delete queue loses its last consumer.
A consumer that closes returns what it took and left unsettled.
A [quorum queue](../subscriptions/rabbit-quorum-queue.md) counts deliveries and dead-letters a spent one through its route.
A waiting queue releases a [delayed](../settlement/delay.md) copy, and [direct reply-to](../request/direct-reply-to.md) gives every request an address of its own.

The transport refuses what the server refuses, with the same error.
That covers a name over 255 bytes, a header the frame cannot carry, and a body over the server's default `max_message_size` of 16 MiB.
It covers the declarations the server refuses: a transient queue that is not exclusive, a binding on the default exchange, a queue declared again with other settings, a quorum queue with a priority.
A delivery settled after its connection closed reports the closed channel.

A publish through the harness returns once every handler it woke has settled, a reply or a slot publish included.
The assertions that follow read finished state.
What only a server has is for a [live test](./live.md): the prefetch window, publisher confirms, the atomicity of a server transaction, a plugin exchange's routing, a queue two connections share, and a topology the service finds without declaring it.

## Example

The dependency line:

```toml
[dev-dependencies]
ruststream-lapin = { version = "0.7", features = ["testing"] }
```

The first test publishes a payment and reads how the handler settled it.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/lapin_testing_in_process.rs#testapp"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/lapin_testing_in_process.rs#testapp"
```

</TabItem>
</Tabs>

The handler sends a receipt to the topic exchange `receipts`, and the queue bound to it under `receipt.*` hands the receipt to its own handler.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/lapin_testing_in_process.rs#exchanges"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/lapin_testing_in_process.rs#exchanges"
```

</TabItem>
</Tabs>

[`tb.out`](api:ruststream::testing::TestApp::out) reads what a slot published, with the [properties](../publishing/properties/index.md) each publish carried.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/lapin_testing_in_process.rs#options"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/lapin_testing_in_process.rs#options"
```

</TabItem>
</Tabs>
