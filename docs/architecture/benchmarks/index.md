---
title: Benchmarks
description: "What ruststream-lapin costs on every message: throughput against the raw lapin client, and instructions and allocations of its code."
---

import {MovedSections, RabbitMqEnvironment, RabbitMqResults} from '@site/src/components/Benchmarks';

export const labels = {"loading": "Loading the results...", "scenario": "Scenario", "raw": "Raw client", "adapter": "Broker crate", "framework": "RustStream", "overhead": "Overhead", "against": "({percent} over the client)", "indistinguishable": "indistinguishable", "brokerBound": "broker-bound", "measured": "measured", "details": "How the numbers were taken", "pending": "No results published yet: {brokers}.", "instructions": "Instructions", "allocations": "Allocations", "cold": "Cold start", "table": "Show the numbers as a table", "axis": "How far each loop falls below the raw client's best round", "rounds": "rounds, best to slowest", "best": "best round", "median": "median round", "noise": "spread of the rounds", "comparison": "Against a raw client", "code": "Cost of the code", "broker": "The broker", "plugin": "The {plugin} plugin", "pluginRaw": "Without the plugin", "pluginAdapter": "Plugin written by hand", "pluginFramework": "The crate's plugin", "pluginAxis": "How far each loop falls below the best round of the app without the plugin", "pluginAgainst": "({percent} over no plugin)", "pluginOverhead": "Over the plugin by hand", "pluginOverHand": "({percent} over by hand)"};

export const machine = {"cpu": "Processor", "architecture": "Architecture", "cpu_frequency": "Frequency", "cores": "Cores", "memory": "Memory", "memory_speed": "Memory speed", "os": "Operating system", "broker": "Broker", "rustc": "Compiler", "profile": "Build profile", "features": "Features", "rustflags": "RUSTFLAGS", "round_trip": "Round trip", "valgrind": "Valgrind"};

# Benchmarks

What `ruststream-lapin` costs on every message: throughput against the raw `lapin` client, and
instructions and allocations of its code.

## Against a raw client

The first chart runs one scenario three ways on a real RabbitMQ node. `Raw client` drives `lapin`
directly, by a loop written by hand. `Broker crate` is the same loop over this crate: the broker,
the queue descriptor, the subscription stream, the delivery's own `ack` and its publisher, with no
runtime around them. `RustStream` is the service a user writes, a `#[subscriber]` handler in an
app, publishing through the same publisher. The first gap is what this crate costs over the client
it wraps, and the second is what the runtime adds on top.

Each loop is a bar from its best round to its slowest, with a dot at the best round and a notch at
the median. The zero of the axis is the raw client's best round, and a dot right of zero is slower.
A hollow dot is a loop whose difference from the raw client is smaller than the spread of the
rounds. The table under the chart prints the best round with the median in parentheses.

The two scenarios are the two queue types RabbitMQ ships. A classic queue holds its messages in
memory and settles a delivery with a single frame. A quorum queue replicates every message through
Raft and writes it down before it hands it out, and a transport that expensive leaves little room
for anything above it to show.

## Cost of the code

The second chart counts what one message costs on the service's thread: instructions under
callgrind and allocations under DHAT, in the steady state. Each scenario is a service on
`LapinBroker` against the same node. The count includes the framework's dispatch, the codec, this
crate's code and the work `lapin` does on that thread. `lapin` reads and writes the socket on a
thread of its own, which is not counted. Most of a row's allocations are the client's: every
acknowledgement and every publish creates promises and an internal task on the service's thread.
`Cold start` is what connecting, opening the channels, declaring the queue, registering the
consumer and taking the first delivery cost once.

## Results

<RabbitMqResults labels={labels} details="/architecture/benchmarks/methodology/" />

## The machine

<RabbitMqEnvironment labels={machine} />

The numbers come from `docs/benchmarks/results.json` of this version, written by the benchmark
recipe. [Methodology](methodology.md) says how they were taken, and [Numbers](numbers.md) what
they allow you to conclude.

<MovedSections />
