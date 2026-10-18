---
sidebar_position: 7
description: "Call another service over RabbitMQ and wait for its answer: a requester over direct reply-to, a responder that replies to it, and a timeout."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Request and reply

This guide makes one service call another over RabbitMQ and wait for the answer.

The orders service asks the inventory service whether an item is in stock before it accepts an order.
The call goes over RabbitMQ's [direct reply-to](https://www.rabbitmq.com/docs/direct-reply-to): no reply queue to declare, and no table of pending calls to keep by hand.

## The responder

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/rpc.rs#responder"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/rpc.rs#responder"
```

</TabItem>
</Tabs>

The inventory service answers with an ordinary handler that returns its reply.
[`DirectReplyTo`](../components/request/direct-reply-to.md) on the reply publisher sends each reply to the address the request named in its `reply-to` property, and copies the request's `correlation-id` onto it.
`Stock` names no destination, because each reply goes where its request asked.
The name in `reply("inventory.check.unanswered")` is where a request without a `reply-to` is answered, and what the service's AsyncAPI document reports.

The routes are a [`Router`](api:ruststream::runtime::Router), so the inventory service mounts them with `b.include_router(inventory())` in its own app.
The example runs both services in one process and mounts both routers.

## The requester

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/rpc.rs#requester"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/rpc.rs#requester"
```

</TabItem>
</Tabs>

`place_order` bounds its slot `Out<impl RequestReply>`, and the mount site pairs it with the [`Request`](../components/request/index.md) policy, [`LapinRequest`](api:ruststream_lapin::LapinRequest).
Each request goes to the default exchange with the routing key `inventory.check`, so it reaches the queue of that name.
It carries `reply-to` set to RabbitMQ's `amq.rabbitmq.reply-to` and a fresh `correlation-id`.
The reply with the same id resolves the call; a reply with another id is dropped.
The request and the reply are bytes here, so `ask_stock` encodes and decodes them with [`JsonCodec`](api:ruststream::codec::JsonCodec).

`RPC_TIMEOUT` bounds the wait.
When nobody answers in time, the call fails with [`AmqpError::RequestTimeout`](../components/broker/errors.md).
`place_order` then asks for the order again in 30 seconds with `retry_after`, instead of guessing the stock.
An item out of stock is a business answer, so the order is dropped.

The policy takes the settings of a publish: [`exchange`](api:ruststream_lapin::LapinRequest::exchange), `priority` and `expiration`.
A request is transient by default: nobody waits for it after the timeout, so it gains nothing from surviving a restart.
[`persistent(true)`](api:ruststream_lapin::LapinRequest::persistent) makes it persistent.

## Delivery guarantees

Direct reply-to is at-most-once.
The reply address lives in the requester's channel on one RabbitMQ node, and nothing is queued durably.
A reply in flight when that channel closes is lost, and the caller sees a timeout.
The request itself is an ordinary message in an ordinary queue: when the inventory service is down, it waits in `inventory.check` and may be answered after the caller has given up.
So the inventory call is safe to repeat: answering it twice changes nothing.

The requester reads its replies on a channel of its own, opened by its first request.

## Another stack on the other side

Both sides speak plain AMQP 0.9.1.
A responder written with another client consumes the request queue, publishes its reply to the default exchange with the routing key from `reply-to`, and copies the `correlation-id`.
A RustStream responder with `DirectReplyTo` answers a requester written with another client the same way.

## Testing

An in-process RabbitMQ belongs to the app that connected it, so the requester and the responder must run in one app to talk.
The tests start the example's app, which mounts both routers, and check that an order in stock is accepted and one out of stock is dropped.
The third test mounts the orders router alone, with a paused clock: nobody answers, the request times out, and the order asks again later.
The in-process RabbitMQ gives every request its own reply address and correlates the reply, as the server does.
[Testing a RabbitMQ service](testing.md) covers the harness.
