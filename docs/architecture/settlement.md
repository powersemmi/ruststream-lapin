---
title: Settlement and retries
sidebar_position: 6
description: "What ack, drop, retry and retry_after become on RabbitMQ, where every settlement is a frame on the delivery's channel."
---

import {Diagram} from '@site/src/components/Diagrams';

export const retries = {
  "title": "A retry and a drop under the retry steps: a classic queue takes dead_letter alone, a quorum queue counts on the server",
  "text": "The diagram as text",
  "outcome": ["The handler returns retry() or drop()", "under the retry steps"],
  "classic": ["RabbitQueue:", "dead_letter only,", "nothing counted"],
  "quorum": ["RabbitQuorumQueue:", "the server counts in", "x-delivery-count"],
  "classicRequeue": ["retry():", "rejected,", "requeued"],
  "classicDropped": ["drop(): the", "server dead-", "letters it"],
  "requeue": ["Below the", "limit:", "rejected,", "requeued"],
  "quorumSpent": ["Past the", "limit: the", "server dead-", "letters it"],
  "arrives": ["Back in the queue", "for another delivery"],
  "plain": ["With nothing declared, retry() is basic.reject", "with requeue, on both queue types"],
  "below": "below"
};

export const delays = {
  "title": "A delayed retry waits in a TTL waiting queue or in a delayed-message exchange, and a queue with neither requeues at once",
  "text": "The diagram as text",
  "outcome": ["The handler returns", "retry_after(delay)"],
  "immediate": ["No delay set:", "requeued at", "once, the delay", "dropped"],
  "waitingQueue": ["Delay::dlx_ttl:", "<queue>.retry", "with a TTL", "per message"],
  "pluginExchange": ["Delay::", "plugin_dme:", "x-delay on a", "plugin exchange"],
  "origin": ["The queue the delivery came from", "the copy is a new message"],
  "restart": ["A copy on the broker outlives the service;", "the service itself holds no copy"],
  "requeued": "requeued",
  "released": "released"
};

# Settlement and retries

What `ack`, `drop`, `retry` and `retry_after` become on RabbitMQ, where every settlement is a
frame on the delivery's channel.

| Outcome | [`RabbitQueue`](api:ruststream_lapin::RabbitQueue) (classic) | [`RabbitQuorumQueue`](api:ruststream_lapin::RabbitQuorumQueue) |
|---|---|---|
| `ack` | `basic.ack`: the message leaves the queue | The same |
| `drop` | `basic.reject` without `requeue`: dead-lettered when the queue has a dead-letter exchange, dropped otherwise | The same |
| `retry()` | `basic.reject` with `requeue`: back in the queue, nothing counted | `basic.reject` with `requeue`: back in the queue, the server counts the delivery |
| `retry()` under `max_attempts` and `dead_letter` | Refused at startup: nothing counts | The server's own delivery limit, below |
| `drop` under `dead_letter` | Dead-lettered to the `dead_letter` queue by the server | The same |
| `retry_after(delay)` | A copy that waits with `delay`, [below](#delayed-retries); an immediate requeue without it | The same |

The crate never sends `basic.nack`. On one delivery it does what `basic.reject` does, and since
RabbitMQ 4.3 only the rejection counts toward a quorum queue's delivery limit. A batch handler's
outcome settles each delivery of the batch with the same frame. A reply that arrives through a
[requester](rabbitmq/reply-to.md) needs no settlement: it comes to a consumer without
acknowledgements.

## Retries

<Diagram name="retries" words={retries} />

`max_attempts(n)` and `dead_letter(name)` at the mount site end a message that keeps failing.
The first delivery counts as one attempt. Both become arguments of the queue, so the server moves
the message and the service publishes no copy of its own.

A classic queue counts nothing, so it takes `dead_letter` alone. The destination becomes the
queue's dead-letter route: `x-dead-letter-exchange` is the default exchange and
`x-dead-letter-routing-key` is the destination, unless the descriptor names an exchange of its
own. A `drop` rejects the delivery without `requeue`, and the server moves it there. A `retry` is
a plain requeue with no count, and `max_attempts` on a classic queue fails the start with an error
that names the quorum queue.

A quorum queue counts its deliveries itself. The pair becomes the queue's `x-delivery-limit`
(`max_attempts` minus one, since the server counts returns) and its dead-letter route to the
`dead_letter` queue. A retry is a plain `basic.reject` with `requeue`, and the server dead-letters
the message once it passes the limit, whether the service is running or not. The pair needs
[`declare_topology(true)`](api:ruststream_lapin::LapinBroker::declare_topology), and the start
fails without it.

The arguments reach a queue when this service declares it. A queue that already exists with other
arguments is refused by the server as inequivalent, and the start fails with the queue and the
arguments in the error. A classic queue this service does not declare keeps the route its
operator gave it. A bare queue name carries no arguments, so it takes neither step. No copy leaves
this process on either queue, so a retry publisher (`out_retry`) does not compile.

## Delayed retries

<Diagram name="delays" words={delays} />

`retry_after(delay)` asks for a new delivery no sooner than `delay`. AMQP has no delay per
message, so the wait happens on the broker, in one of two places.

With no [`delay`](api:ruststream_lapin::RabbitQueue::delay) on the queue nothing waits: the
delivery is requeued at once, and a warning says the delay was dropped.

[`Delay::dlx_ttl`](api:ruststream_lapin::Delay::dlx_ttl) keeps the copy on the broker. The crate
publishes it to a waiting queue, `<queue>.retry` by default, with the delay as its per-message
expiration, and acknowledges the original after it on the same channel. When the expiration
passes, the waiting queue dead-letters the copy back to the origin queue. A queue releases expired
messages only from its head, so a short delay behind a long one waits for the long one; one waiting
queue per delay keeps them apart.

[`Delay::plugin_dme`](api:ruststream_lapin::Delay::plugin_dme), under the `plugin-dme` feature,
publishes the copy to an exchange of the delayed-message plugin, `<queue>.delay` by default, with
the delay in its `x-delay` header. The plugin routes it to the origin queue once the delay has
passed, and delays of different lengths do not wait for each other.

Either target is infrastructure. Under `declare_topology(true)` each subscription declares its
own. Without declaration the subscription checks at startup that the target exists, and a missing
one fails the start, because the server would drop a copy sent to a waiting queue that does not
exist. The copy is a new message, so the server counts it from zero, and a quorum queue's limit
ends immediate retries only. It carries the `x-ruststream-retry-count` header raised by one.

The rules on the server side are RabbitMQ's
([negative acknowledgements](https://www.rabbitmq.com/docs/confirms),
[quorum queue delivery limit](https://www.rabbitmq.com/docs/quorum-queues#poison-message-handling),
[dead lettering](https://www.rabbitmq.com/docs/dlx),
[per-message TTL](https://www.rabbitmq.com/docs/ttl),
[delayed message exchange](https://github.com/rabbitmq/rabbitmq-delayed-message-exchange)).
