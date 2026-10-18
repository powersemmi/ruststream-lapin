---
sidebar_position: 1
title: ConfirmsPublish
description: "ConfirmsPublish, TransactionalPublish in the prelude, resolves each publish once the broker confirmed it and commits by awaiting every confirm."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# ConfirmsPublish

[`ConfirmsPublish`](api:ruststream_lapin::ConfirmsPublish), `TransactionalPublish` in the prelude, is the publish policy whose every publish resolves once the broker has confirmed the message.

[`LapinPublish::confirms`](api:ruststream_lapin::LapinPublish::confirms) reaches it with the same settings: exchange and property defaults.
It pairs into a [`ConfirmsPublisher`](api:ruststream_lapin::ConfirmsPublisher) with a confirm channel of its own.
A message the broker refuses fails the publish, and the handler decides what follows.
RabbitMQ confirms a message once it has routed it, and a message that matches no queue is confirmed and dropped.
So a confirm says the broker took the message, not that a queue holds it.

Inside a transaction the publisher buffers in the client.
`commit` publishes the buffer in order and resolves once every message is confirmed, and `abort` discards the buffer without touching the broker.
A failed commit loses the buffer, and the delivery's redelivery is the way to recover.
The policy offers the borrowed kind of the [transaction scope](./index.md) and the owned kind of [`ConfirmsTransaction`](./confirms-transaction.md).

## Example

A payment is acknowledged once the broker has confirmed its receipt. A receipt the broker refused brings the payment back.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/lapin_transactions_confirms.rs#confirms"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/lapin_transactions_confirms.rs#confirms"
```

</TabItem>
</Tabs>
