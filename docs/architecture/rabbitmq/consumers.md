---
title: Consumers and prefetch
sidebar_position: 2
description: "A queue pushes deliveries to its consumers, each within a window of unacknowledged deliveries that its acknowledgements reopen."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "A queue pushes deliveries to two consumers, each with a prefetch window of two; an ack frees a slot, and a closed channel returns its deliveries",
  "text": "The diagram as text",
  "queue": ["Queue orders", "messages wait in order"],
  "consumer1": ["Consumer 1", "prefetch 2:", "2 unacknowledged"],
  "consumer2": ["Consumer 2", "prefetch 2:", "2 unacknowledged"],
  "ack": ["basic.ack frees", "a slot, and the next", "delivery is pushed"],
  "closed": ["Channel closed:", "its unacknowledged", "deliveries return,", "marked redelivered"],
  "roundRobin": ["The queue spreads deliveries over its consumers in turn,", "skipping a consumer whose window is full"],
  "deliver": "basic.deliver"
};

# Consumers and prefetch

A queue pushes deliveries to its consumers, each within a window of unacknowledged deliveries that
its acknowledgements reopen.

<Diagram name="consumers" words={words} />

A consumer does not ask for messages. It registers with `basic.consume`, and the queue pushes
`basic.deliver` frames to it. Several consumers on one queue compete: the queue gives each message
to one of them, in turn.

The prefetch window is the number of deliveries a consumer may hold unacknowledged. Once the
window is full, the queue pushes nothing more to that consumer until an acknowledgement frees a
slot. This is how a slow handler slows the queue down instead of filling the service's memory.
RabbitMQ sets the window with `basic.qos`, per consumer, and reads 0 as no limit.

A delivery stays the consumer's until it is settled. `basic.ack` removes the message from the
queue. `basic.reject` with `requeue` puts it back, and without `requeue` the queue drops it or
dead-letters it. When a channel closes with deliveries still unsettled, the queue takes them back
and delivers them again with the `redelivered` flag set.

## In this crate

Each subscription opens a channel of its own and one consumer on it. Before the consumer
starts, the subscription sends `basic.qos` with its window:
[`RabbitQueue::prefetch`](api:ruststream_lapin::RabbitQueue::prefetch) for one queue, or
[`LapinBroker::prefetch`](api:ruststream_lapin::LapinBroker::prefetch) for every subscription
that sets none. The setting is a non-zero number, so a subscription that sets neither sends no
`basic.qos` and runs with no limit.

The window also bounds a batch. AMQP has no batch frame, so a batch handler's batch is collected
on the client from what the queue has already pushed: up to the size the mount site names, or
what arrived within [`batch_wait`](api:ruststream_lapin::RabbitQueue::batch_wait) (50
milliseconds by default). A window smaller than the batch size caps the batch at the window.

A dropped subscription closes its channel, and RabbitMQ requeues what it held unsettled. How
the runtime settles each delivery is on [Settlement and retries](../settlement.md).

The protocol rules are RabbitMQ's
([consumer prefetch](https://www.rabbitmq.com/docs/consumer-prefetch),
[consumer acknowledgements](https://www.rabbitmq.com/docs/confirms#consumer-acknowledgements),
[work queues](https://www.rabbitmq.com/tutorials/tutorial-two-python)).

## Connection recovery

With [connection recovery](../../components/broker/connection.md#automatic-recovery) enabled, Lapin restores the consumer's existing channel, declared exchanges, queues and bindings, and reapplies its prefetch before resuming delivery.
The stream waits through recoverable disconnects and ends with a consume error if retries run out. With recovery disabled, it follows the plain Lapin consumer's terminal behavior.
Unsettled deliveries from queues that survive the disconnect can be redelivered. An old acknowledgement can fail after reconnect; the handler must tolerate processing a delivery again.
Failed publishes are not replayed, and outstanding RPC deadlines keep running.
