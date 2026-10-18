---
title: LapinRequest
description: "LapinRequest, Request in the prelude, gives a handler a requester over direct reply-to that awaits the correlated reply or times out."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# LapinRequest

[`LapinRequest`](api:ruststream_lapin::LapinRequest), `Request` in the prelude, gives a handler a requester: it publishes a request over RabbitMQ direct reply-to and awaits the correlated reply.

The handler bounds its slot `Out<impl RequestReply>` and calls [`request`](api:ruststream::RequestReply::request) with a message and a timeout.
At startup the runtime pairs the policy into a [`LapinRequester`](api:ruststream_lapin::LapinRequester).
Each request goes out with `reply-to` set to the `amq.rabbitmq.reply-to` pseudo-queue and a generated `correlation-id`.
The reply that carries that id resolves the call, and a reply with an id nobody waits for is dropped.
No reply within the timeout fails the call with [`AmqpError::RequestTimeout`](api:ruststream_lapin::AmqpError::RequestTimeout).
One task per requester reads the replies, started by its first request on the runtime the broker connected on.
The request carries bytes, so the example encodes the question and decodes the answer with the core's JSON codec.

The policy takes [`exchange`](api:ruststream_lapin::LapinRequest::exchange), [`priority`](api:ruststream_lapin::LapinRequest::priority) and [`expiration`](api:ruststream_lapin::LapinRequest::expiration) like a publish policy.
Requests are transient by default, and [`persistent(true)`](api:ruststream_lapin::LapinRequest::persistent) makes them persistent.
Direct reply-to is at-most-once: the reply state lives on the requester's channel on one node, and nothing is queued durably.
A channel that drops loses the replies in flight, and the caller sees a timeout.
The responder is any service that answers at the received `reply-to` with the same `correlation-id`, as [`DirectReplyTo`](./direct-reply-to.md) does.

## Example

An order asks the inventory service whether its item is in stock. An order in stock is accepted, one out of stock is dropped, and no answer brings the order back.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/lapin_request_request.rs#request"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/lapin_request_request.rs#request"
```

</TabItem>
</Tabs>

The app mounts the requester's policy on the handler's slot. The inventory service runs in the same app here.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/lapin_request_request.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/lapin_request_request.rs#app"
```

</TabItem>
</Tabs>
