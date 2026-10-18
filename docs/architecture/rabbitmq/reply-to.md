---
title: Direct reply-to
sidebar_position: 5
description: "Direct reply-to sends a reply straight to the requester's channel with no reply queue, at most once."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "A request over direct reply-to: the request goes through an ordinary queue, the reply comes straight back to the requester's channel",
  "text": "The diagram as text",
  "consume": ["Requester channel consumes", "amq.rabbitmq.reply-to, no acks"],
  "request": ["Request published on that channel", "with reply-to and a correlation-id"],
  "requests": ["Request queue", "an ordinary queue"],
  "responder": ["Responder handler", "DirectReplyTo on its reply"],
  "reply": ["Reply on the default exchange", "routing key = the reply-to it got"],
  "back": ["Requester channel", "receives it, no queue between"],
  "resolved": ["The request resolves", "matched by its correlation-id"],
  "atMostOnce": ["Replies live in the requester's channel:", "a reply sent after that channel is gone is lost"]
};

# Direct reply-to

Direct reply-to sends a reply straight to the requester's channel with no reply queue, at most
once.

<Diagram name="replyTo" words={words} />

The requester consumes the pseudo-queue `amq.rabbitmq.reply-to` in no-ack mode. It needs no
declaration. A request published on the same channel with `reply-to` set to that name gets a
private address: RabbitMQ rewrites the property to `amq.rabbitmq.reply-to.` followed by a suffix
of its own.

The request travels like any message, through an exchange into a queue, to a consumer. The
responder publishes its reply to the default exchange, with the `reply-to` it received as the
routing key. RabbitMQ hands the reply directly to the requester's channel. No queue holds it.

A reply is delivered at most once. If the requester's channel is gone, the reply is dropped, and
the requester has to send the request again.

## In this crate

[`LapinRequester`](api:ruststream_lapin::LapinRequester) opens a channel of its own on the first
request, starts the reply consumer on it, and publishes every request on that channel. Each
request carries a generated `correlation-id`, and the reply with the same id resolves the call.
One task per requester reads the replies, on the runtime the broker connected on. A channel
error closes the channel and loses the replies in flight; the next request opens a new one. The
per-request timeout is what ends a request whose reply never comes.

Requests are transient by default: a request nobody waits for after its timeout gains nothing from
surviving a restart. [`LapinRequest::persistent`](api:ruststream_lapin::LapinRequest::persistent)
turns persistence on.

The responder is an ordinary handler with a reply.
[`DirectReplyTo`](api:ruststream_lapin::DirectReplyTo), mounted as a transform of its reply,
sends each reply to the address the request named and echoes its `correlation-id`. A reply arrives
on a consumer without acknowledgements, so settling it does nothing.

The protocol rules are RabbitMQ's
([direct reply-to](https://www.rabbitmq.com/docs/direct-reply-to)).
