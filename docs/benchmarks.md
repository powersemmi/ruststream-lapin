# Benchmarks

Reaching a handler from a RabbitMQ queue costs time on every message: the subscription stream, the
decode, the dispatch, the ack. This page says how much, and splits the cost in two: what this crate
adds to the `lapin` client it wraps, and what the runtime adds on top of that.

Three loops in one process run the same scenario, and they differ in one thing each: what carries
the messages. Everything else is held equal - the connection, the channel a subscription is given,
the queue arguments, the prefetch window, the position of the ack, the decode into the same type,
the payload bytes, the tokio runtime and the build. The procedure is the framework's own and is
described on the
[RustStream benchmarks page](https://powersemmi.github.io/ruststream/latest/benchmarks/#methodology);
this page publishes what it produced here.

## The numbers

The best of three interleaved rounds, with the slowest round in parentheses. Higher is better.

<div id="benchmark-results" data-benchmark-labels='{"loading": "Loading the published results...", "scenario": "Scenario", "raw": "Raw client", "adapter": "This crate", "framework": "Full service", "adapterOverhead": "This crate over raw", "overhead": "Full service over raw", "indistinguishable": "indistinguishable", "brokerBound": "broker-bound", "machine": "Machine", "os": "OS", "broker": "Broker", "roundTrip": "Round trip", "build": "Build", "versions": "Versions", "measured": "Measured", "unavailable": "No results could be read. They are published at {url}.", "unknownSchema": "The published results declare schema {schema}, which this page does not render."}'></div>

The table is read in your browser from the document the last run wrote, so nothing on this page is
a copy that could have gone stale.

`Raw client` is `lapin` driven directly, by a loop written by hand. `This crate` is the same loop
over `ruststream-lapin` itself: the broker, the queue descriptor, the subscription stream, the
delivery's own `ack` and its publisher, with no service around them. `Full service` is what a user
writes, a `#[subscriber]` handler under the app and the runtime, publishing through the same
publisher.

So `This crate over raw` is what the adapter in this repository costs, which is the figure this
repository answers for. The step from it to `Full service over raw` is what the runtime costs on
top, over RabbitMQ in particular: the framework's own cost is measured and published by the core,
and this row says how it lands on this transport.

The two rows are the two queue implementations RabbitMQ ships, and each sets those costs against a
different transport. A classic queue holds its messages in memory and settles a delivery with a
single frame. A quorum queue replicates every message through Raft and writes it down before it
hands it out, and a transport that expensive leaves little room for anything above it to show.

An overhead reported as `indistinguishable` is one where that column and the raw client differ by
less than the spread between runs of either. A figure below the run-to-run noise would read as
precision that was never measured, so none is published.

A row marked `broker-bound` is one where the raw client spent the run waiting on the server rather
than working. The mark is decided by measurement, not by a guess: a probe outside the rounds times one
request the server answers, and the mark goes on when the round trips a delivery costs add up to at
least half the time a message took. That round trip is published under the machine below, so the
arithmetic can be checked. An AMQP delivery waits for no round trip of its own, because the server
pushes the body and answers no acknowledgement; what it waits for is the credit an acknowledgement
returns, and the prefetch window spreads that wait over every message in it.

The machine-readable form of the same run, which the framework's site reads to build its
cross-broker table, is at
[`benchmarks/results.json`](https://powersemmi.github.io/ruststream-lapin/latest/benchmarks/results.json).

## The machine

<div id="benchmark-environment"></div>

The build flags are published with the numbers because they change them: a binary built with
`-C target-cpu=native` produces a figure no other machine can reproduce, so the recipe clears the
variable before it builds.

## What they do not mean

This is one consumer on one queue, a small body, and a node on the loopback. It measures what a
delivery costs in this crate, not what RabbitMQ can carry, and a row here is not comparable with a
row published for another broker: the transports do different work per message.

The prefetch window decides the absolute figures. A consumer that acknowledges every message and is
allowed one unsettled delivery at a time spends a round trip per message, and all three loops come
out at the speed of that round trip. The wide window these runs use is what lets the costs above
the transport show at all, and moving it moves every column together.

Every run declares a queue of its own and deletes it afterwards, so no loop works through what the
run before it left behind. The bodies are published without asking for persistence: a classic
queue keeps them in memory, and the quorum row carries RabbitMQ's own durability because a quorum
queue has no other mode.

The window a run measures opens at the first delivery and closes when the work on the last one is
done, in all three loops alike. Every loop acknowledges a delivery after that point, so one
acknowledgement out of the millions a run carries sits outside the number in each of them.

The numbers are a snapshot of one machine on one day. They are re-measured on demand, never in CI:
a shared runner's noise is larger than the difference this page is about.

## Running it yourself

```bash
just bench
```

The recipe starts the node from `docker-compose.test.yml`, runs both scenarios, stops it again and
rewrites `docs/benchmarks/results.json` with what it measured. It takes a quarter of an hour and
wants the machine to itself. The message count is not fixed: a probe run sets it so that every measured
run lasts at least five seconds on whatever machine it is taken on.
