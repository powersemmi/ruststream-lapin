---
title: Design
sidebar_position: 2
description: "Where ruststream-lapin sits: between the RustStream runtime and the lapin client, one connection, a channel per subscription."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "The service, the RustStream runtime, this crate, the lapin client and the RabbitMQ node, top to bottom",
  "text": "The diagram as text",
  "service": "Your service",
  "handlers": ["#[subscriber]", "handlers"],
  "app": ["RustStream::new(..)", ".with_broker(..)"],
  "core": "RustStream core",
  "runtime": "Runtime: dispatch, lanes, settlement",
  "crate": "ruststream-lapin",
  "subscriber": ["LapinSubscriber", "yields LapinMessage"],
  "publishers": ["LapinPublisher,", "ConfirmsPublisher,", "ServerTxPublisher"],
  "broker": ["LapinBroker", "configuration"],
  "descriptors": ["RabbitQueue,", "RabbitQuorumQueue,", "RabbitExchange"],
  "requester": ["LapinRequester"],
  "client": "lapin",
  "connection": ["Connection", "one per broker"],
  "subChannels": ["A channel per", "subscription"],
  "pubChannels": ["Shared publish channel;", "confirms, transactions", "and requests: their own"],
  "rabbitmq": "RabbitMQ node",
  "mounted": "mounted",
  "polls": "polls the stream"
};

# Design

Where `ruststream-lapin` sits: between the RustStream runtime and the lapin client, one
connection, a channel per subscription.

<Diagram name="design" words={words} />

The crate implements the core's broker traits over [lapin](https://docs.rs/lapin/4.12.0/lapin/), a
pure-Rust AMQP 0.9.1 client. The runtime above it is the same for every broker. Everything
RabbitMQ-specific is in this crate: the [queue descriptors](rabbitmq/queues.md), the frames a
settlement sends, where a retry goes, and the three [publishing modes](rabbitmq/publishing.md).

One broker opens one connection. Each subscription consumes on a channel of its own, so its
prefetch window and its errors stay its own. Fire-and-forget publishers share one channel. A
confirms publisher, a transaction publisher and a requester each open a channel of their own on
first use, because a confirm mode, a transaction and a reply consumer are channel state.

A channel error closes one channel and leaves the connection up. A publisher's channel opens again
on its next use, so one publish to a missing exchange does not stop the service publishing.

[`LapinBroker`](api:ruststream_lapin::LapinBroker) declares nothing by default. A descriptor then
states the topology the service expects, and a missing queue fails its subscription.
[`declare_topology(true)`](api:ruststream_lapin::LapinBroker::declare_topology) makes each
subscription declare its exchanges, its queue and its bindings, in that order.

A requester reads its replies in a task on the runtime the broker connected on, whichever thread
makes the first request.

How the pieces start and stop is on [Lifecycle](lifecycle.md), and the way of one delivery on
[Path](path.md).
