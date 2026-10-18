---
title: Model
sidebar_position: 0
description: "RabbitMQ routes a message through an exchange into queues by bindings, and a consumer reads one queue over a channel of a connection."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "A producer publishes to an exchange, a binding copies the message into a queue, and a consumer reads the queue; clients talk over channels of one connection",
  "text": "The diagram as text",
  "producer": ["Producer", "publishes with a routing key"],
  "vhost": "Virtual host",
  "exchange": ["Exchange", "picks the queues by its bindings"],
  "queue": ["Queue", "holds messages until a consumer settles them"],
  "consumer": ["Consumer", "reads one queue"],
  "connection": "One TCP connection",
  "channel1": "Channel 1",
  "channel2": "Channel 2",
  "channelN": "Channel n",
  "channels": ["Every publish and every consumer runs on a channel:", "a light session the connection multiplexes"],
  "publishes": "publish",
  "binds": "binding",
  "delivers": "deliver"
};

# Model

RabbitMQ routes a message through an exchange into queues by bindings, and a consumer reads one
queue over a channel of a connection.

<Diagram name="model" words={words} />

A producer never writes to a queue. It publishes to an exchange and gives the message a routing
key. The exchange compares the message with its bindings, and every queue whose binding matches
gets a copy. A message that matches no binding reaches no queue.

A queue is storage that a consumer empties. A message leaves the queue once its consumer
acknowledges it. Until then the server keeps it, and a delivery the consumer gives back or never
settles returns to the queue.

Every server has a default exchange with an empty name. Each queue is bound to it under its own
name, so a publish to the default exchange with the routing key `orders` lands in the queue
`orders`. That is how a service with no topology of its own still publishes to a queue.

Exchanges, queues and bindings live in a virtual host, which the connection URI names. A client
opens one TCP connection and runs its work on channels inside it. A channel is a light session
with state of its own: a prefetch window, a confirm mode, a transaction. An error on a channel
closes that channel and leaves the connection up.

The model is AMQP 0.9.1 as RabbitMQ implements it
([AMQP concepts](https://www.rabbitmq.com/tutorials/amqp-concepts),
[channels](https://www.rabbitmq.com/docs/channels),
[the default exchange](https://www.rabbitmq.com/docs/exchanges#default-exchange)). How each
exchange type routes is on [Exchange types](routing.md), how a queue pushes to its consumers on
[Consumers and prefetch](consumers.md), and how the two queue types differ on
[Classic and quorum queues](queues.md).
