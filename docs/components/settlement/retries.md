---
sidebar_position: 2
title: Retries
description: "max_attempts and dead_letter become queue arguments: a quorum queue takes both, a classic queue takes dead_letter alone."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Retries

[`max_attempts(n)`](api:ruststream::runtime::Mounting::max_attempts) and [`dead_letter(name)`](api:ruststream::runtime::Mounting::dead_letter) end a message that keeps failing, and RabbitMQ applies both itself.

`max_attempts(n)` is how many deliveries one message gets, the first included.
`dead_letter(name)` is where it goes when the queue carries it away.
Each step becomes an argument of the queue, so the server moves the message and the service publishes nothing.

| Queue | `max_attempts(n)` | `dead_letter(name)` |
|---|---|---|
| [`RabbitQuorumQueue`](../subscriptions/rabbit-quorum-queue.md) | `x-delivery-limit` of `n - 1` | dead-letter route to the queue `name` |
| [`RabbitQueue`](../subscriptions/rabbit-queue.md) | refused at startup | dead-letter route to the queue `name` |
| a bare queue name | refused at startup | refused at startup |

A quorum queue counts the deliveries a message spends, so it takes the pair: the message that reaches the cap leaves for `name`.
A classic queue counts nothing, so it has no cap; the service refuses to start with `max_attempts(..)` on one, and the error names the quorum queue.
On a classic queue, `dead_letter(name)` takes the messages the handler drops.
A bare queue name carries no queue arguments, so it takes neither step; the error names the descriptor to mount instead.
The route is the queue's own, written as `x-dead-letter-exchange` (the default exchange) and `x-dead-letter-routing-key` (`name`), unless the descriptor names a [dead-letter exchange](./dead-letter-exchange.md) of its own.

The arguments reach the queue when the service declares it with [`declare_topology(true)`](api:ruststream_lapin::LapinBroker::declare_topology).
A queue that already exists with other arguments is refused by the server, and the service fails to start with the queue and the arguments in the error.
On a broker that declares nothing, a classic queue keeps the route its operator gave it, and a quorum queue refuses the pair.
[`out_retry(policy)`](api:ruststream::runtime::Mounting::out_retry) does not compile on either queue: no copy leaves this process.

## Example

A declined card is retried at once. The quorum queue delivers it three times and then moves it to `payments.dead`, where a second subscription reads it.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/lapin_settlement_retries.rs#retries"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/lapin_settlement_retries.rs#retries"
```

</TabItem>
</Tabs>
