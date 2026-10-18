---
sidebar_position: 4
title: Shutdown
description: "A publisher or requester that outlives the connection reports AmqpError::Closed instead of succeeding against a closed connection."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Shutdown

A publisher or requester that outlives the connection reports [`AmqpError::Closed`](api:ruststream_lapin::AmqpError::Closed) instead of succeeding against a closed connection.

At shutdown the runtime stops the subscriptions and lets the handlers in flight finish.
Then it closes the connection with the AMQP close handshake.
A delivery the service received and never settled goes back to its queue when its channel closes, so another consumer gets it.

A publisher handed out before the shutdown can outlive it: a task the service spawned keeps it, or a clone of a [requester](../request/index.md) does.
Every use of such a handle after the shutdown returns `AmqpError::Closed`, which names the routing key, the queue or the exchange the call targeted.
The connection is marked closed before the handshake starts, so no message slips into a connection that is going away.
A tool that drives the broker by hand receives a [`ClosedLapinBroker`](api:ruststream_lapin::ClosedLapinBroker) from the shutdown; its [`handshake`](api:ruststream_lapin::ClosedLapinBroker::handshake) tells an orderly close from a connection the server had already dropped.

## Example

A heartbeat task takes its publisher at startup and runs past the service's shutdown; its first publish after the shutdown fails with `AmqpError::Closed`, and the task ends.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/lapin_broker_shutdown.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/lapin_broker_shutdown.rs#app"
```

</TabItem>
</Tabs>
