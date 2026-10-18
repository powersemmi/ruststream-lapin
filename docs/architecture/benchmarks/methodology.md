---
title: Methodology
description: "How ruststream-lapin measures itself: the three loops, the run on a real node, the broker-bound arithmetic and the code count."
---

# Methodology

How `ruststream-lapin` measures itself: the three loops, the run on a real node, the broker-bound
arithmetic and the code count. The procedure is the framework's own
([RustStream methodology](https://powersemmi.github.io/ruststream/latest/architecture/benchmarks/methodology/)),
and this page states what it is on RabbitMQ.

## The three loops

One process runs each scenario three times over, as three loops that differ in one thing: what
carries the messages.

- **Raw client.** `lapin`, driven directly by a loop written by hand: consume, decode, ack.
- **Broker crate.** This crate and nothing above it: the broker connects, the queue descriptor
  opens a subscription, and a loop pulls deliveries off its stream, decodes them and acks them.
- **RustStream.** The service a user writes: a `#[subscriber]` handler and the app around it.

Everything else is equal across the three: the connection, the channel a subscription is given,
the queue arguments, the prefetch window, the place of the ack, the decode into the same type, the
payload bytes, the tokio runtime and the binary.

The body is 512 bytes of JSON, published without asking for persistence. A classic queue keeps it
in memory; a quorum queue has no other mode than its own replicated storage.

## The run

- **A fresh queue per run.** Every run declares a queue of its own and deletes it afterwards, so
  no loop works through what the run before it left behind.
- **The window opens at the first delivery and closes when the work on the last one is done**, in
  all three loops. Every loop acknowledges a delivery after that point, so one acknowledgement out
  of the millions a run carries sits outside the number in each of them.
- **The message count comes from a probe**, which sets it so that every measured run lasts at least
  five seconds on any machine.
- **The loops are interleaved**: raw, crate, RustStream, three rounds. Each loop reports its best,
  median and worst round.

A row is `broker-bound` when the raw client spent the run waiting on the server rather than
working. That is arithmetic: a probe outside the rounds times one `basic.qos` the server answers,
and the row is flagged when the round trips a delivery costs add up to at least half the time a
message took. The round trip is published with the machine, so the arithmetic can be checked. An
AMQP delivery waits for no round trip of its own, because the server pushes the body and answers no
acknowledgement. What it waits for is the credit an acknowledgement returns, and the prefetch
window spreads that wait over every message in it.

## The code count

Each scenario of the [cost chart](index.md#cost-of-the-code) is the service a user writes, on
`LapinBroker` connected to the node of the compose stand, built without the `testing` feature. The
service declares a classic queue at start, consumes it with the prefetch window of the comparison,
and runs on a single-threaded tokio runtime. The queue is filled from another thread before the
service handles anything, and the fill waits until the node has confirmed every message.

Instructions come from callgrind and allocations from DHAT. A scenario runs over one delivery,
then 1000, then 2000. The difference between the last two is the steady cost of a message, and the
single delivery is the cold start.

A real node answers in its own time, so how often the service's thread waits for the socket changes
from run to run. Over seven runs of an unchanged tree a run's instructions moved by up to 2.4
percent, and its allocations by a block at most. A scenario's allocation floor sits a tenth of a
percent above the highest count it produced, and `just bench-code` fails on an allocation above it.
With `--baseline=main` the recipe also fails on more than five percent more instructions; a plain
run applies no instruction limit. A failed run still prints the table, and under it every limit it
breached, the old value beside the new one.

## Running it yourself

```bash
just bench
```

The recipe starts the node from `docker-compose.test.yml`, runs both comparison scenarios, stops
the node and rewrites `docs/benchmarks/results.json`. It takes a few minutes and wants the machine
to itself. It clears `RUSTFLAGS`, so the binary is not tied to the CPU it was built on.
Both recipes build the benchmarks the way a release is built, with fat link-time optimization
across crates, so the numbers describe the code a service ships.

```bash
just bench-code
```

The code count takes under a minute and needs valgrind. The recipe installs the benchmark runner
itself, at the release `Cargo.lock` pins. A number after the recipe's name sets the deliveries per
measured run, as in `just bench-code 2000`; the published numbers are taken at the default of 1000.
`just bench-code --save-baseline=main` on `main` records a baseline, and
`just bench-code --baseline=main` on a branch compares with it.
