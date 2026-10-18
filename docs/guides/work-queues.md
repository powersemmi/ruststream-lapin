---
sidebar_position: 2
description: "Share a RabbitMQ queue between workers: competing consumers, prefetch, keyed lanes for per-key order, batches and a single active consumer."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Work queues

This guide shares one RabbitMQ queue between the workers of a service and keeps the order where it matters.

The example is an invoices service.
Every instance reads the same `invoices` queue and renders what it takes.
A second queue feeds an archive in batches, and a third feeds the ledger in strict queue order.

## Competing consumers

Every instance of the service subscribes to the same queue.
RabbitMQ hands each message to one of them, in turn, and never to two at once.
Starting another instance adds a consumer, and the queue drains faster.
A message an instance took and did not settle goes back to the queue when its connection closes, and another instance takes it.

The service needs nothing for this: the queue name in `#[subscriber]` is the same in every instance.

## Prefetch

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/work_queues.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/work_queues.rs#app"
```

</TabItem>
</Tabs>

[`prefetch`](../components/subscriptions/prefetch.md) caps the deliveries a subscription holds without an acknowledgement, here 32.
RabbitMQ sends a consumer nothing more while it holds that many.
So a slow instance stops taking work, and the next message goes to an instance with room for it.
Without a prefetch, RabbitMQ pushes the queue to its consumers as fast as the network allows, and a slow instance piles up messages the others could have taken.
The count is a `NonZeroU16`: leaving it unset is how no limit is spelled.
The broker's value applies to every subscription, and a queue descriptor overrides it for its own queue.

The window is the server's, so a test sees it only against a running RabbitMQ.

## Lanes and per-key order

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/work_queues.rs#lanes"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/work_queues.rs#lanes"
```

</TabItem>
</Tabs>

`workers(8, by_key)` handles up to eight invoices of one instance at the same time, on eight [lanes](../components/subscriptions/lanes.md).
The billing service that publishes an invoice puts the customer in the `amqp-partition-key` header ([`PARTITION_KEY_HEADER`](api:ruststream_lapin::PARTITION_KEY_HEADER)).
Invoices with the same key go to the same lane, one after another, in the order the instance received them.
Invoices of different customers run in parallel.
An invoice without the header goes to the next lane in turn.
RabbitMQ does not read the header: the lanes are the service's own convention.

The order holds inside one instance.
Two instances each take part of a customer's invoices, so their lanes do not order across instances.
The handler reads [`Redelivered`](../components/context/index.md) to log whether RabbitMQ delivered the invoice before.

## Batches

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/work_queues.rs#batches"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/work_queues.rs#batches"
```

</TabItem>
</Tabs>

A slice parameter asks for a [batch](../components/subscriptions/batches.md), and the mount site names its size with `.batch(nonzero!(64))`.
AMQP delivers one message at a time, so the batch is collected in the service.
It closes when it holds 64 invoices or when [`batch_wait`](api:ruststream_lapin::RabbitQueue::batch_wait) has passed since its first invoice; the wait defaults to 50 ms.
A batch holds only what RabbitMQ has already sent, so the archive queue raises its own prefetch to 64.
With the broker's 32, no batch would ever reach 64.
The handler's outcome settles every invoice of the batch.

## One consumer at a time

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/work_queues.rs#single_active"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/work_queues.rs#single_active"
```

</TabItem>
</Tabs>

The ledger needs every invoice in queue order, across all instances.
RabbitMQ's [single active consumer](https://www.rabbitmq.com/docs/consumers#single-active-consumer) gives that: the queue delivers to one consumer, and another takes over when it goes away.
The descriptor has no method for it, so [`argument`](../components/subscriptions/arguments.md) passes `x-single-active-consumer` to the queue declaration as it is.
The other instances stay subscribed and idle until they take over.

The argument is part of the queue's declaration.
Under `declare_topology(true)` the service declares the queue with it; a queue that operations create carries it from their declaration ([Topology](topology.md)).

## Testing

The tests in the example file publish keyed invoices with `with_headers` and check that the invoices of one customer reach the handler in order.
The in-process RabbitMQ shares a queue between competing consumers and assembles batches as a server does.
The prefetch window and single active consumer are the server's, so they belong to a [live test](../components/testing/live.md).
[Testing a RabbitMQ service](testing.md) covers the harness.
