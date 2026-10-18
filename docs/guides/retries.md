---
sidebar_position: 5
description: "Settle RabbitMQ deliveries with native frames, retry now or after a delay, cap the attempts and move what keeps failing to a dead-letter queue."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Acknowledgements, retries and dead letters

This guide settles RabbitMQ deliveries, retries them now or later, caps the attempts and moves what keeps failing to a [dead-letter queue](../components/settlement/retries.md).

The example is a refunds service.
Refunds arrive on a classic queue, payouts on a quorum queue, and both go to a dead-letter queue after five deliveries.

## What an outcome sends

A handler's [outcome](../components/settlement/index.md) becomes an AMQP frame on the delivery's channel:

| Outcome | Frame | What RabbitMQ does |
|---|---|---|
| `HandlerOutcome::ack()` | `basic.ack` | removes the message from the queue |
| `HandlerOutcome::retry()` | `basic.reject` with requeue | puts the message back in the queue |
| `HandlerOutcome::drop()` | `basic.reject` without requeue | removes the message, or dead-letters it where the queue has a [dead-letter exchange](../components/settlement/dead-letter-exchange.md) |
| `HandlerOutcome::retry_after(delay)` | an acknowledged delivery and a delayed copy | delivers the copy again once the delay has passed |

RabbitMQ counts every rejection as a delivery the message has spent.
So a handler that keeps asking for its message back runs a quorum queue's own limit down.
A delivery the service took and never settled goes back to the queue when the channel or the connection closes.

## The subscriber

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/retries.rs#refund"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#refund"
```

</TabItem>
</Tabs>

The card processor refuses a refund in three ways, and each needs a different outcome.
`Busy` means its rate limit is used up: a retry right now would be refused again, so [`retry_after`](../components/settlement/retry-after.md) waits 30 seconds.
`Unavailable` means it did not answer: `retry` tries again at once.
`Declined` means the card account is closed, and another try gets the same answer, so the refund is dropped.

## The cap on a classic queue

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/retries.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#app"
```

</TabItem>
</Tabs>

[`max_attempts`](api:ruststream::runtime::RouterWith::max_attempts) counts every delivery of one refund, the first included.
The fifth delivery is not retried: [`dead_letter`](api:ruststream::runtime::RouterWith::dead_letter) publishes it to `refunds.dead`.
A destination is a routing key on the default exchange, so the refund lands in the queue of that name.

A classic queue counts nothing, so the [cap](../components/settlement/retries.md) is the service's.
Under a cap, a retry acknowledges the delivery and publishes a copy back to the queue under its own name.
The copy carries the attempt count in the `x-ruststream-retry-count` header ([`RETRY_COUNT_HEADER`](api:ruststream::runtime::RETRY_COUNT_HEADER)), raised by one.
The copies leave through the broker's default publish policy; `.out_retry(policy)` names another, for example [`TransactionalPublish`](../components/transactions/confirms-publish.md) to wait for RabbitMQ's confirm of each copy.

A dropped refund does not reach `refunds.dead`: the dead-letter queue holds only the refunds that ran out of attempts.
To keep dropped messages too, give the queue its own dead-letter exchange ([Topology](topology.md)).

## The cap on a quorum queue

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/retries.rs#payout"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#payout"
```

</TabItem>
</Tabs>

A quorum queue counts the deliveries of each message itself.
On `payouts`, the same two steps at the mount site become the queue's `x-delivery-limit` and its dead-letter route.
The server then moves a payout to `payouts.dead` after its fifth delivery, whether or not a service is running, and the service publishes nothing.
A retry stays a plain `basic.reject` with requeue.

The arguments reach the queue only where the service declares it, so this needs [`declare_topology(true)`](../components/broker/declare-topology.md).
On a broker that declares nothing, the service refuses to start: a queue declared elsewhere carries its owner's arguments, and the cap would apply nowhere.
There, set the limit and the route on the queue itself, by its declaration or a policy, and mount the handler without the two steps.
Half a declaration is refused too, because a limit with nowhere to send the spent message drops it.
`.out_retry(..)` does not compile on a quorum queue: there is no copy to publish.

A quorum queue declared without a limit has RabbitMQ 4's default of 20 deliveries; [`delivery_limit`](../components/settlement/delivery-limit.md) sets another on the descriptor.
A quorum delivery reports how many deliveries it has spent through `redelivery_count()`.
A classic delivery reports only [`Redelivered`](../components/context/index.md), whether RabbitMQ handed it out before.

## Delayed retries

AMQP has no per-message delay.
Without a `delay` on the queue, `retry_after` holds the copy in the service and publishes it when the delay has passed.
That copy is at-most-once: a service that stops during the delay loses it.

[`delay(Delay::dlx_ttl())`](../components/settlement/delay.md) moves the wait to RabbitMQ.
The copy waits in the queue `refunds.retry` under a per-message TTL, and the waiting queue dead-letters it back to `refunds` when the TTL fires.
The original is acknowledged after the copy is published on the same channel, so a failure between the two duplicates the refund rather than losing it.
The copy is a new message, so it carries the retry count raised by one, and the cap ends a delayed loop as it ends an immediate one.

A waiting queue releases messages only from its head.
A message with a long delay holds back every message behind it, so mixed delays want one waiting queue per delay, named with [`Delay::dlx_ttl_named`](api:ruststream_lapin::Delay::dlx_ttl_named).
The `plugin-dme` feature adds [`Delay::plugin_dme`](../components/settlement/plugin-dme.md), which waits in RabbitMQ's delayed-message-exchange plugin, where a short delay never waits behind a long one.

The waiting queue and the delayed exchange are infrastructure.
Under `declare_topology(true)` the service declares them.
With declaration off, a subscription with a `delay` checks at startup that its waiting queue or its delayed exchange exists, and the service does not start without it.

## Reading the dead-letter queue

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/retries.rs#dead_letters"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#dead_letters"
```

</TabItem>
</Tabs>

A dead-letter queue is an ordinary queue, and a handler reads it like any other.
A refund the service dead-lettered carries the retry-count header with the number of deliveries it went through, 5 here.
A payout the quorum queue dead-lettered carries RabbitMQ's own `x-death` header instead.
Both handlers log the message for a person to look at.

## Testing

The tests in the example file run the app in process with a paused clock, so `tb.advance(BACKOFF)` lets the 30 seconds pass at once.
The in-process RabbitMQ releases the waiting queue's copy, counts a quorum queue's deliveries and dead-letters a spent one through its route, as the server does.
The delayed-message-exchange plugin works only on a server, so its tests run against a live RabbitMQ.
[Testing a RabbitMQ service](testing.md) covers the harness.
