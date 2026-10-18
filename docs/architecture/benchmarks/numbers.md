---
title: Numbers
description: "What the benchmark numbers of ruststream-lapin measure, and what they do not allow you to conclude."
---

# Numbers

What the benchmark numbers of `ruststream-lapin` measure, and what they do not allow you to
conclude.

## The comparison against a raw client

The three loops of a row are comparable with each other, because they ran in one process,
interleaved, on one node. A row is not comparable with a row another broker crate publishes: the
transports do different work per message.

An overhead reported as `indistinguishable` is one where that loop and the raw client differ by less
than the spread between runs of either. A figure below the run-to-run noise would read as precision
that was never measured, so none is published.

A `broker-bound` row is one where the raw client waited on the server for most of the run. The work
of the crate and the runtime then happens inside that wait, and the difference comes out near zero.
That is a lower bound on the cost, not a measurement of it.

## The cost of the code

An instruction count holds this crate's code, the runtime above it and the work `lapin` does on the
service's thread. The counts are absolute, the framework's own cost included; the core publishes
that cost alone on its
[benchmarks page](https://powersemmi.github.io/ruststream/latest/architecture/benchmarks/). The same
count costs more time where it misses the cache, so a count is not a time.

Every figure is the steady state, the difference between a run of 1000 deliveries and one of 2000.
Connecting, opening the channels, declaring the queue and registering the consumer are the cold
start, paid once.

## What they do not mean

A row is one consumer on one queue, a small body, and a node on the loopback. It measures what a
delivery costs in this crate, not what RabbitMQ can carry.

The prefetch window decides the absolute figures. A consumer that acknowledges every message and is
allowed one unsettled delivery at a time spends a round trip per message, and all three loops come
out at the speed of that round trip. The wide window these runs use is what lets the costs above
the transport show at all, and moving it moves every column together.

The numbers are a snapshot of one machine on one day, with the build recorded beside them. They are
measured again by hand, on a machine given to the run alone: the differences they show are smaller
than the noise of a shared one.
