---
title: Settlement and retries
sidebar_position: 6
description: "What ack, drop, retry and retry_after become on RabbitMQ, where every settlement is a frame on the delivery's channel."
---

import {Diagram} from '@site/src/components/Diagrams';

export const retries = {
  "title": "A retry under max_attempts and dead_letter: a classic queue leaves the count to the runtime, a quorum queue counts on the server",
  "text": "The diagram as text",
  "outcome": ["The handler returns retry()", "under max_attempts and dead_letter"],
  "classic": ["RabbitQueue:", "the runtime counts in", "x-ruststream-retry-count"],
  "quorum": ["RabbitQuorumQueue:", "the server counts in", "x-delivery-count"],
  "copy": ["Below the", "cap: a copy", "to the queue"],
  "classicSpent": ["At the cap:", "a copy to", "the dead-", "letter queue"],
  "requeue": ["Below the", "limit:", "rejected,", "requeued"],
  "quorumSpent": ["Past the", "limit: the", "server dead-", "letters it"],
  "arrives": ["Back in the queue", "for another delivery"],
  "plain": ["With neither declared, retry() is basic.reject", "with requeue, on both queue types"],
  "below": "below"
};

export const delays = {
  "title": "A delayed retry waits in the service process, in a TTL waiting queue or in a delayed-message exchange, and each sends the copy back to the queue",
  "text": "The diagram as text",
  "outcome": ["The handler returns", "retry_after(delay)"],
  "process": ["No delay set:", "a timer in", "the service", "process"],
  "waitingQueue": ["Delay::dlx_ttl:", "<queue>.retry", "with a TTL", "per message"],
  "pluginExchange": ["Delay::", "plugin_dme:", "x-delay on a", "plugin exchange"],
  "origin": ["The queue the delivery came from", "the copy is a new message"],
  "restart": ["The timer in the process is lost if the process dies;", "a copy on the broker outlives the service"],
  "republished": "republished",
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
| `retry()` under `max_attempts` and `dead_letter` | The runtime publishes a counted copy, below | The server's own delivery limit, below |
| `retry_after(delay)` | A copy that waits, [below](#delayed-retries) | The same with `delay`; an immediate requeue without it |

The crate never sends `basic.nack`. On one delivery it does what `basic.reject` does, and since
RabbitMQ 4.3 only the rejection counts toward a quorum queue's delivery limit. A batch handler's
outcome settles each delivery of the batch with the same frame. A reply that arrives through a
[requester](rabbitmq/reply-to.md) needs no settlement: it comes to a consumer without
acknowledgements.

## Retries

<Diagram name="retries" words={retries} />

`max_attempts(n)` and `dead_letter(name)` at the mount site end a message that keeps failing.
The first delivery counts as one attempt. Who counts depends on the queue type.

A classic queue counts nothing, so the runtime does. On a retry below the cap it rejects the
original without `requeue` and publishes a copy to the default exchange under the queue's name,
which lands at the back of the same queue. The copy carries the attempt number in the
[`x-ruststream-retry-count`](api:ruststream::runtime::RETRY_COUNT_HEADER) header. At the cap the
copy goes to the queue named by `dead_letter` instead, or, with no `dead_letter`, the delivery is
rejected and nothing is published. On a classic queue that has a dead-letter exchange of its own,
the rejection of each original reaches that exchange too.

A quorum queue counts its deliveries itself. The pair becomes the queue's `x-delivery-limit`
(`max_attempts` minus one, since the server counts returns) and its dead-letter route to the
`dead_letter` queue. A retry is a plain `basic.reject` with `requeue`, and the server dead-letters
the message once it passes the limit, whether the service is running or not. The arguments reach
the queue only when this service declares it, so the pair needs
[`declare_topology(true)`](api:ruststream_lapin::LapinBroker::declare_topology), and the start
fails without it. A quorum queue publishes no copies, so a retry publisher (`out_retry`) does not
compile on it.

## Delayed retries

<Diagram name="delays" words={delays} />

`retry_after(delay)` asks for a new delivery no sooner than `delay`. AMQP has no delay per
message, so the wait happens in one of three places.

With no [`delay`](api:ruststream_lapin::RabbitQueue::delay) on a classic queue, the runtime holds
the copy. It rejects the original and publishes the copy when a timer in the service fires. A
graceful shutdown waits for the timer, and a process that dies before it fires loses the copy. On
a quorum queue without `delay` the runtime has no copy to hold: the delivery is requeued at once,
and a warning says the delay was dropped.

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
exist. The copy is a new message, so the server counts it from zero. It carries the
`x-ruststream-retry-count` header raised by one, and on a classic queue the runtime reads the cap
from it before the copy is sent: a delivery at the cap goes to the dead-letter queue instead of
waiting again.

The rules on the server side are RabbitMQ's
([negative acknowledgements](https://www.rabbitmq.com/docs/confirms),
[quorum queue delivery limit](https://www.rabbitmq.com/docs/quorum-queues#poison-message-handling),
[dead lettering](https://www.rabbitmq.com/docs/dlx),
[per-message TTL](https://www.rabbitmq.com/docs/ttl),
[delayed message exchange](https://github.com/rabbitmq/rabbitmq-delayed-message-exchange)).
