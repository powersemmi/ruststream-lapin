---
sidebar_position: 4
description: "Decide who creates RabbitMQ queues and exchanges: the service with declare_topology, or operations, with classic and quorum queues and raw arguments."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Topology

This guide describes a service's queues, exchanges and bindings, and decides who creates them: the service or the people who operate RabbitMQ.

A queue descriptor states what the service expects to find on the server.
Nothing is created unless the broker opts in with [`declare_topology(true)`](../components/broker/declare-topology.md).
The example is a payments service with a replicated queue for payments, a queue for the payments that fail, and a queue of each instance's own for exchange rates.

## Classic and quorum queues

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/topology.rs#queues"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/topology.rs#queues"
```

</TabItem>
</Tabs>

RabbitMQ has two queue types, and the type is fixed when the queue is created.
A [classic queue](../components/subscriptions/rabbit-queue.md) lives on one node.
A [quorum queue](../components/subscriptions/rabbit-quorum-queue.md) is replicated over the cluster by Raft and survives the loss of a node.
Each type has its own descriptor: [`RabbitQueue`](api:ruststream_lapin::RabbitQueue) and [`RabbitQuorumQueue`](api:ruststream_lapin::RabbitQuorumQueue).
Take a quorum queue for data that must not be lost, such as payments, and a classic queue for the rest.
RabbitMQ's [quorum queue guide](https://www.rabbitmq.com/docs/quorum-queues) compares the two.

A quorum queue is always durable, shared between connections and permanent.
So `RabbitQuorumQueue` has no `durable`, `exclusive` or `auto_delete`, and a quorum queue that contradicts its type does not compile.
A quorum queue also counts the deliveries of each message, which the [retries guide](retries.md) builds on.

[`dead_letter_exchange`](../components/settlement/dead-letter-exchange.md) gives `payments` a route for what it rejects.
A payment `charge` drops goes to the `payments.dead` exchange, and from there to `payments.failed`, the queue bound to it.
[`dead_letter_routing_key`](api:ruststream_lapin::RabbitQueue::dead_letter_routing_key) replaces the message's routing key on that route when the exchange routes by key.

## A queue of each instance

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/topology.rs#instance"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/topology.rs#instance"
```

</TabItem>
</Tabs>

Every instance needs every exchange rate, so each instance reads a queue of its own, bound to the `fx-rates` fanout exchange.
The queue name carries the process id, so two instances never share it.
[`exclusive(true)`](api:ruststream_lapin::RabbitQueue::exclusive) lets only this connection use the queue, and RabbitMQ deletes it when the connection closes.
[`auto_delete(true)`](api:ruststream_lapin::RabbitQueue::auto_delete) deletes it once its last consumer is gone.
[`durable(false)`](api:ruststream_lapin::RabbitQueue::durable) keeps it off the disk: rates are worthless after a restart.
RabbitMQ 4 accepts a transient queue only when it is exclusive, so `durable(false)` comes with `exclusive(true)`.

[`argument`](../components/subscriptions/arguments.md) passes a declaration argument the descriptor has no method for, here `x-message-ttl`: a rate nobody read within a minute expires.
The same way sets `x-max-length`, `x-max-priority`, `x-overflow` or `x-single-active-consumer`.
The value is an [`AMQPValue`](api:ruststream_lapin::AMQPValue), and [`arguments`](api:ruststream_lapin::RabbitQueue::arguments) takes a whole table.

## Declared by the service

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/topology.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/topology.rs#app"
```

</TabItem>
</Tabs>

With `declare_topology(true)`, each subscription declares at startup what its descriptor states.
It declares the exchanges its bindings name first, then the queue with its arguments, then the bindings.
The default exchange and the built-in `amq.*` exchanges exist on every server and are never declared.
A declaration repeats harmlessly while it matches what the server has, so every instance of the service declares the same queues.

A declaration that differs from the existing queue fails.
RabbitMQ answers `PRECONDITION_FAILED` to a queue declared again with another durability or other arguments, and the service does not start: [`AmqpError::Declare`](../components/broker/errors.md) names the queue.
A queue's type and arguments change by deleting and declaring it again, which loses what it holds, so they change through a migration, not a deploy.
RabbitMQ explains which properties must match in [property equivalence](https://www.rabbitmq.com/docs/queues#property-equivalence).

## Owned by operations

`DECLARE_TOPOLOGY=false` starts the same service with declaration off.
The descriptors then declare nothing: they state the queues, arguments and bindings the service expects, and operations create them.
A queue that does not exist fails the subscription at startup with [`AmqpError::Subscribe`](../components/broker/errors.md), so a missing queue stops the deploy instead of losing messages.

Operations often set limits and TTLs with a RabbitMQ [policy](https://www.rabbitmq.com/docs/policies) instead of declaration arguments.
A policy changes a queue without declaring it again, and an argument set by a policy never conflicts with a service's declaration.
The descriptor then names only what the service itself relies on.

Declaration off also changes what a quorum queue's retry declaration can do, which the [retries guide](retries.md) describes.

## Testing

The tests in the example file reject a payment and find it in `payments.failed`, under declaration on and off.
The in-process RabbitMQ follows the broker's setting: it declares what the descriptors state and refuses what the server refuses, such as a transient queue that is not exclusive, a binding on the default exchange or a queue declared again with other settings.
With declaration off, every queue a subscription names is there in process, so a missing queue belongs to a [live test](../components/testing/live.md).
[Testing a RabbitMQ service](testing.md) covers the harness.
