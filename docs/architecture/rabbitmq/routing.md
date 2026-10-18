---
title: Exchange types
sidebar_position: 1
description: "Each exchange type reads a different part of a message to pick its queues: the routing key, a pattern over it, nothing, or the headers."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "One message with routing key order.eu.placed and the header region=eu, offered to five exchange types, and the queues each one picks",
  "text": "The diagram as text",
  "message": ["A message: routing key order.eu.placed,", "header region = eu"],
  "exchanges": "Exchange types",
  "direct": ["direct", "key equals the binding"],
  "directQueues": ["Queues bound with", "order.eu.placed"],
  "topic": ["topic", "key matches a pattern"],
  "topicQueues": ["Queues bound with", "order.*.placed or order.#"],
  "fanout": ["fanout", "key ignored"],
  "fanoutQueues": ["Every queue", "bound to it"],
  "headers": ["headers", "x-match = all or any"],
  "headersQueues": ["Queues whose binding", "asks region = eu"],
  "hash": ["x-consistent-hash", "plugin: hash of the key"],
  "hashQueues": ["One queue, chosen in", "proportion to its weight"],
  "offered": "published to"
};

# Exchange types

Each exchange type reads a different part of a message to pick its queues: the routing key, a
pattern over it, nothing, or the headers.

<Diagram name="exchanges" words={words} />

A `direct` exchange copies the message into every queue bound with a key equal to its routing key.
The default exchange is a `direct` exchange, with every queue bound under its own name.

A `topic` exchange reads the routing key as words separated by dots. A binding is a pattern over
those words: `*` stands for exactly one word and `#` for zero or more. The key `order.eu.placed`
matches `order.*.placed` and `order.#`, and does not match `order.*`.

A `fanout` exchange ignores the key and copies the message into every queue bound to it.

A `headers` exchange ignores the key too. It compares the message's headers with the binding's
arguments. With `x-match` set to `all` every argument has to match, with `any` one is enough. In
this crate a binding with arguments is
[`bind_with`](api:ruststream_lapin::RabbitQueue::bind_with).

A message can match several bindings. It then reaches every matching queue, once per queue.

The consistent-hash exchange comes from a plugin that ships with RabbitMQ and has to be enabled.
It hashes the routing key and sends the message to one bound queue. A binding's key is its
weight: a queue bound with weight 2 gets about twice the keys of a queue bound with weight 1.
Messages with one routing key go to one queue in normal operation; a node restart can move a key
to another queue. The crate offers
it as [`RabbitExchange::consistent_hash`](api:ruststream_lapin::RabbitExchange::consistent_hash)
under the `plugin-consistent-hash` feature, and [Lanes and order](../lanes.md) compares it with
lanes inside one subscription.

[`RabbitExchange`](api:ruststream_lapin::RabbitExchange) names the exchange of a binding.
[`custom`](api:ruststream_lapin::RabbitExchange::custom) names any other type a plugin adds.

The routing rules are RabbitMQ's
([exchanges](https://www.rabbitmq.com/docs/exchanges),
[topic exchanges](https://www.rabbitmq.com/tutorials/tutorial-five-python),
[consistent hash exchange](https://github.com/rabbitmq/rabbitmq-server/tree/main/deps/rabbitmq_consistent_hash_exchange)).
