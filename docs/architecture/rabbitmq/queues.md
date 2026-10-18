---
title: Classic and quorum queues
sidebar_position: 3
description: "A classic queue lives on one node and counts nothing; a quorum queue is replicated, counts failed deliveries and moves a spent message itself."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "Classic and quorum queues side by side: where each keeps its messages, what a requeue does, and what ends a message that keeps coming back",
  "text": "The diagram as text",
  "classic": "Classic queue",
  "quorum": "Quorum queue",
  "classicStore": ["One node", "holds the messages"],
  "classicRequeue": ["reject with requeue:", "back in the queue,", "nothing counted"],
  "classicReject": ["reject without", "requeue: dead-", "lettered or dropped"],
  "quorumStore": ["A leader and its", "followers, by Raft"],
  "quorumRequeue": ["reject with requeue:", "back in the queue,", "x-delivery-count + 1"],
  "quorumSpent": ["Past the delivery", "limit: dead-", "lettered or dropped"],
  "limit": ["Since RabbitMQ 4.0 a quorum queue's delivery limit", "is 20 unless the queue sets another"],
  "spent": "limit passed"
};

# Classic and quorum queues

A classic queue lives on one node and counts nothing; a quorum queue is replicated, counts failed
deliveries and moves a spent message itself.

<Diagram name="queues" words={words} />

The type is fixed when the queue is declared, through the `x-queue-type` argument. Both types
take the same consumers and the same settlement frames. They differ in where the messages live
and in what a requeue costs a message.

In RabbitMQ 4 a classic queue keeps its messages on one node, unreplicated. A rejected delivery
with `requeue` goes back and can come round again without end. The queue keeps no count: a
redelivery carries only the `redelivered` flag.

A quorum queue replicates its messages over several nodes with the Raft protocol, and a quorum of
them accepts each write. It is durable by definition. It counts the failed deliveries of each
message and stamps the count on a redelivery as the `x-delivery-count` header. A message whose
count passes the queue's delivery limit leaves the queue. Since RabbitMQ 4.0 the limit is 20 when
the queue sets none.

Since RabbitMQ 4.3 not every return counts. `basic.reject` and a channel that closes with the
delivery unsettled count. `basic.nack` with `requeue` returns the message without counting it.

A message that leaves a queue by a rejection without `requeue`, or past the delivery limit, goes to
the queue's dead-letter exchange when it has one (`x-dead-letter-exchange`, with an optional
`x-dead-letter-routing-key`). A queue with none drops it.

## In this crate

The queue type is a descriptor type, because it decides what a retry does:
[`RabbitQueue`](api:ruststream_lapin::RabbitQueue) for a classic queue and
[`RabbitQuorumQueue`](api:ruststream_lapin::RabbitQuorumQueue) for a quorum one. A quorum queue
descriptor has no `durable`, `exclusive` or `auto_delete` step, since a quorum queue is always
durable and shared.

The crate settles a negative outcome with `basic.reject`, never with `basic.nack`. On one delivery
the two frames do the same thing, and only the rejection counts on a quorum queue. A handler's
`retry()` therefore spends one of the message's deliveries, and the queue's limit ends a message
that keeps failing. [Settlement and retries](../settlement.md) draws how each type answers a
retry.

The queue rules are RabbitMQ's
([classic queues](https://www.rabbitmq.com/docs/classic-queues),
[quorum queues](https://www.rabbitmq.com/docs/quorum-queues),
[poison message handling](https://www.rabbitmq.com/docs/quorum-queues#poison-message-handling),
[dead letter exchanges](https://www.rabbitmq.com/docs/dlx)).
