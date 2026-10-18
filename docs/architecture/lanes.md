---
title: Lanes and order
sidebar_position: 5
description: "One subscription consumes one queue on one channel; workers(n, by_key) spreads its deliveries over lanes keyed by a header."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "One subscription's dispatch: a queue, the subscription's channel, one stream and two lanes keyed by amqp-partition-key",
  "text": "The diagram as text",
  "queue": ["Queue orders", "one consumer of this service"],
  "channel": ["Subscription channel", "a prefetch window of unacknowledged deliveries"],
  "stream": ["LapinSubscriber stream", "one delivery at a time"],
  "keyed": ["workers(2, by_key)", "lane = hash of the amqp-partition-key header"],
  "lane0": ["Lane 1", "keys a and c,", "each in order"],
  "lane1": ["Lane 2", "key b, in order"],
  "hash": ["On the server instead: a consistent-hash exchange", "spreads keys over queues, a subscription each"],
  "pushed": "pushed",
  "oneAtATime": "one at a time"
};

# Lanes and order

One subscription consumes one queue on one channel; `workers(n, by_key)` spreads its deliveries
over lanes keyed by a header.

<Diagram name="lanes" words={words} />

A subscription's stream yields one delivery at a time, in the order the queue pushed them. Without
`workers`, one loop runs the handler for each delivery in turn, so the queue's order holds and
nothing runs side by side. The prefetch window decides how many deliveries wait in the service
meanwhile.

`workers(n, by_key)` runs `n` lanes. A delivery goes to the lane its key hashes to, and a lane runs
its deliveries in order. The key is the
[`amqp-partition-key`](api:ruststream_lapin::PARTITION_KEY_HEADER) header, which the producer sets.
AMQP does not read it: it is a convention between the producer and this crate. A delivery without
the header has no key, and such deliveries rotate over the lanes.

Lanes need a window wider than one. With a prefetch of 1 the queue pushes the next delivery only
after the last one settles, and the lanes take turns.

The order a lane keeps is the order inside one consumer. Two instances of the service on one
queue are two competing consumers, and the queue spreads the deliveries of one key over both.
RabbitMQ's single active consumer (`x-single-active-consumer`), a queue argument set with
[`argument`](api:ruststream_lapin::RabbitQueue::argument), lets one consumer read the queue at a
time.

The [consistent-hash exchange](rabbitmq/routing.md) moves the same idea to the server. It spreads
the routing keys over several queues, and each queue gets a subscription of its own. One key goes
to one queue in normal operation, so its order holds across instances while each queue has one
consumer.

A [batch handler](rabbitmq/consumers.md) takes what the queue has already pushed, up to the size
the mount site names, so a batch is often smaller than that size.
