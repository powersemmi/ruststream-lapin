---
sidebar_position: 4
title: Arguments
description: "argument and arguments pass any x- declaration argument the descriptor does not model, such as a TTL or a length limit, verbatim."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Arguments

[`argument`](api:ruststream_lapin::RabbitQueue::argument) and [`arguments`](api:ruststream_lapin::RabbitQueue::arguments) pass any `x-` declaration argument the descriptor does not model, such as a TTL or a length limit, verbatim.

A queue declaration carries a table of arguments, and RabbitMQ reads queue features from it: `x-message-ttl`, `x-max-length`, `x-overflow`, `x-single-active-consumer` and more.
`argument(name, value)` adds one entry, and `arguments(table)` replaces the whole table.
The value is an [`AMQPValue`](api:ruststream_lapin::AMQPValue) and the table a [`FieldTable`](api:ruststream_lapin::FieldTable), both re-exported from `lapin`.
A name is at most 255 bytes, the protocol's limit for a field name.
The settings the descriptor models, such as the [dead-letter exchange](../settlement/dead-letter-exchange.md) and the queue type, travel in the same table, so they are written with their own steps.

The arguments reach the server in the queue's declaration, so they take effect where the broker [declares the topology](../broker/declare-topology.md).
A queue keeps the arguments it was first declared with.
A declaration with other arguments is refused with `PRECONDITION_FAILED`, and startup fails with [`AmqpError::Declare`](api:ruststream_lapin::AmqpError::Declare); the [in-process transport](../testing/index.md) refuses it the same way.
What an argument does to the messages, such as a TTL that expires them, is the server's work and shows only against a server.

## Example

Notifications expire after a minute, and the queue holds at most ten thousand of them.
The macro form adds the arguments one by one; the manual form builds the table.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/lapin_subscriptions_arguments.rs#arguments"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/lapin_subscriptions_arguments.rs#arguments"
```

</TabItem>
</Tabs>
