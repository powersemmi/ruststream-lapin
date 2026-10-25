---
sidebar_position: 5
description: "Settle RabbitMQ deliveries with native frames, retry now or after a delay, cap the attempts and move what keeps failing to a dead-letter queue."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Acknowledgements, retries and dead letters

This guide settles RabbitMQ deliveries, retries them now or later, caps the attempts and moves what keeps failing to a [dead-letter queue](../components/settlement/retries.md).

The example is a refunds service.
Refunds arrive on a classic queue, and a refund with a declined card goes to a dead-letter queue.
Payouts arrive on a quorum queue, which moves a payout to a dead-letter queue after five deliveries.

## What an outcome sends

A handler's [outcome](../components/settlement/index.md) becomes an AMQP frame on the delivery's channel:

| Outcome | Frame | What RabbitMQ does |
|---|---|---|
| `HandlerOutcome::ack()` | `basic.ack` | removes the message from the queue |
| `HandlerOutcome::retry()` | `basic.reject` with requeue | puts the message back in the queue |
| `HandlerOutcome::drop()` | `basic.reject` without requeue | removes the message, or dead-letters it where the queue has a [dead-letter exchange](../components/settlement/dead-letter-exchange.md) |
| `HandlerOutcome::retry_after(delay)` | an acknowledged delivery and a copy in the queue's waiting queue | delivers the copy again once the delay has passed |

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
`Busy` means its rate limit is used up, and `Unavailable` means it did not answer.
A retry right now would fail again, so [`retry_after`](../components/settlement/retry-after.md) waits 30 seconds.
`Declined` means the card account is closed, and another try gets the same answer, so the refund is dropped.

## Dead letters on a classic queue

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

[`dead_letter`](api:ruststream::runtime::RouterWith::dead_letter) gives `refunds` a dead-letter route to `refunds.dead`.
A destination is a routing key on the default exchange, so a dropped refund lands in the queue of that name.
The route is an argument of the queue, so RabbitMQ moves the refund and the service publishes nothing.

A classic queue counts nothing, so it has no cap.
A `retry` requeues the refund as many times as the handler asks.
[`max_attempts`](api:ruststream::runtime::RouterWith::max_attempts) on a classic queue stops the service at startup, and the error names the quorum queue, which has a cap.

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
`.out_retry(..)` does not compile on either queue: there is no copy to publish.

A quorum queue declared without a limit has RabbitMQ 4's default of 20 deliveries; [`delivery_limit`](../components/settlement/delivery-limit.md) sets another on the descriptor.
A quorum delivery reports how many deliveries it has spent through `redelivery_count()`.
A classic delivery reports only [`Redelivered`](../components/context/index.md), whether RabbitMQ handed it out before.

## Delayed retries

AMQP has no per-message delay.
[`delay(Delay::dlx_ttl())`](../components/settlement/delay.md) gives the queue one.
Without a `delay`, `retry_after` requeues the delivery at once, drops the delay and logs a warning.

The copy waits in the queue `refunds.retry` under a per-message TTL, and the waiting queue dead-letters it back to `refunds` when the TTL fires.
The original is acknowledged after the copy is published on the same channel, so a failure between the two duplicates the refund rather than losing it.
The copy is a new message, which the server counts from zero, so it carries the framework's retry count raised by one instead.
On a quorum queue that means a delayed retry starts the queue's count again, and `max_attempts` ends only immediate retries.

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
RabbitMQ moved both messages, so each carries its `x-death` header: the queue it left and the reason.
Both handlers log the message for a person to look at.

## Testing

The tests in the example file run the app in process with a paused clock, so `tb.advance(BACKOFF)` lets the 30 seconds pass at once.
The in-process RabbitMQ releases the waiting queue's copy, counts a quorum queue's deliveries and dead-letters a dropped or spent message through its queue's route, as the server does.
The delayed-message-exchange plugin works only on a server, so its tests run against a live RabbitMQ.
[Testing a RabbitMQ service](testing.md) covers the harness.
