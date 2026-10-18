---
title: Path
sidebar_position: 4
description: "The way of one RabbitMQ delivery through a RustStream service: the push, the handler, the reply and the acknowledgement."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "The path of one RabbitMQ delivery that is answered, from the queue to its acknowledgement",
  "text": "The diagram as text",
  "queue": ["Queue", "pushes within the prefetch window"],
  "deliver": ["basic.deliver", "on the subscription's own channel"],
  "message": ["LapinMessage", "body kept, headers read on arrival"],
  "dispatch": ["Dispatch", "one loop, or a lane per key"],
  "decode": ["Decode", "the codec reads the body"],
  "body": ["Handler", "async fn under #[subscriber]"],
  "encode": ["Reply", "encoded into the loop's buffer"],
  "publisher": ["LapinPublisher", "basic.publish of the reply"],
  "settle": ["Settlement", "basic.ack or basic.reject"]
};

# Path

The way of one RabbitMQ delivery through a RustStream service: the push, the handler, the reply
and the acknowledgement.

<Diagram name="path" words={words} />

The queue pushes the delivery to the subscription's consumer while its
[prefetch window](rabbitmq/consumers.md) has room. lapin hands it over with the body in a buffer
of its own. [`LapinMessage`](api:ruststream_lapin::LapinMessage) keeps that buffer as the body,
without a copy. It reads the headers when the delivery arrives: the string and byte entries of the
header table, plus the native properties a handler reads as headers (`content-type`,
`correlation-id`, `reply-to`, `message-id`, the priority and the expiration). The exchange, the routing key, the `redelivered`
flag and the delivery tag ride beside them as the delivery's context.

The runtime takes the delivery from the subscription's stream and runs it in its
[dispatch loop](lanes.md). The codec decodes the body, and the handler runs.

A reply is encoded into a buffer the dispatch loop reuses. The
[publisher](rabbitmq/publishing.md) lends those bytes to lapin, which copies them into the frame
it sends. The four identity headers go out in their native properties, so a consumer that is not a
RustStream service finds them where AMQP puts them. Every other header goes into the header table
as a byte string. The routing key is the reply's destination, and the exchange is the publish
policy's: the default exchange unless the policy names another.

The delivery settles after the reply, on the channel it arrived on. `ack` sends `basic.ack`.
`retry()` and `drop` send `basic.reject`, with and without `requeue`. A reply that fails to
publish settles the delivery as a retry. [Settlement and retries](settlement.md) draws what each
outcome becomes.

What this path costs per message, against the raw lapin client, is on
[Benchmarks](benchmarks/index.md).
