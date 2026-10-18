---
title: Confirms and transactions
sidebar_position: 4
description: "A publish to RabbitMQ ends when the frame is written, when the server confirms it, or when a channel transaction commits."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "Three ways a publish ends: fire-and-forget when the frame is written, confirms when the server answers, a transaction at the commit",
  "text": "The diagram as text",
  "fireAndForget": "Fire and forget",
  "confirms": "Confirms",
  "transaction": "Transaction",
  "ffPublish": "basic.publish",
  "ffDone": ["Returns once", "the frame is", "written"],
  "confirmSelect": ["confirm.select", "once"],
  "confirmPublish": "basic.publish",
  "confirmAck": ["basic.ack or", "basic.nack for", "each message"],
  "txSelect": ["tx.select", "once"],
  "txPublish": ["basic.publish", "one or more"],
  "txCommit": "tx.commit",
  "txVisible": ["commit-ok:", "every publish", "visible"],
  "unroutable": ["A message no queue takes is confirmed and dropped:", "the crate publishes without the mandatory flag"]
};

# Confirms and transactions

A publish to RabbitMQ ends when the frame is written, when the server confirms it, or when a
channel transaction commits.

<Diagram name="publishing" words={words} />

`basic.publish` has no answer of its own. A publisher that sends it and moves on learns nothing
about the message: a missing exchange or a queue that fails goes unseen.

Publisher confirms add the answer. After `confirm.select` the channel numbers its publishes, and
the server answers each one with `basic.ack` once it has taken responsibility for it. For a
persistent message in a durable queue that means written to disk; for a quorum queue it means
accepted by a quorum of replicas. `basic.nack` says the server could not handle the message, and
it is rare: it comes from an internal error of a queue.

A message that matches no binding is confirmed too. The exchange finds no queue, the server
confirms the message and drops it. Only the `mandatory` flag makes the server return such a
message to the publisher.

A channel transaction stages publishes on the server. After `tx.select`, publishes wait until
`tx.commit`; `commit-ok` says every one of them is visible, and `tx.rollback` discards them. Each
commit is a round trip that waits for the server. RabbitMQ does not promise atomicity when a node
fails during the commit.

## In this crate

The publish policy decides the mode, and a stronger mode is a different policy:

| Policy | Channel | A publish returns |
|---|---|---|
| [`LapinPublish`](api:ruststream_lapin::LapinPublish) | one channel shared by every such publisher of the connection | once the frame is written |
| [`ConfirmsPublish`](api:ruststream_lapin::ConfirmsPublish) | its own, in confirm mode | once the server confirms; a negative confirm is an error |
| [`ServerTxPublish`](api:ruststream_lapin::ServerTxPublish) | its own, in transaction mode | inside a transaction, at the commit |

A confirms or transaction channel opens on the first publish, so a policy that never publishes
holds no channel. Every publish goes out without `mandatory`, so a confirmed publish can still
have reached no queue: a confirm says the server took the message, not that a queue holds it.
Messages are persistent unless the policy says otherwise.

A channel error closes the channel and leaves the connection up. A fire-and-forget publish to an
exchange that does not exist is enough, and the publisher does not see the server's answer. The
next publish opens a new channel. A transaction channel that dies with a transaction open is the
exception: the transaction lived on it, so the commit fails instead of reporting success.

The transaction of [`ConfirmsPublish`](api:ruststream_lapin::ConfirmsPublish) is a buffer in the
service. The commit publishes the buffered messages in order and waits for every confirm. It is
durable per message and not atomic: messages sent before a failed one stay published. The
transaction of [`ServerTxPublish`](api:ruststream_lapin::ServerTxPublish) is RabbitMQ's own. It
is one per channel, because `tx.select` puts the channel itself in transaction mode.

The protocol rules are RabbitMQ's
([publisher confirms](https://www.rabbitmq.com/docs/confirms),
[transactions](https://www.rabbitmq.com/docs/semantics)).
