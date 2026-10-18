---
sidebar_position: 4
title: retry_after
description: "retry_after redelivers no sooner than a delay; with no delay on the queue the service holds the copy, at most once over the window."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# retry_after

[`retry_after(delay)`](api:ruststream::runtime::HandlerOutcome::retry_after) asks for a redelivery no sooner than `delay`, for a message that is not ready yet.

AMQP has no per-message delay, so on a queue without a [delay](./delay.md) the runtime waits.
It settles the delivery, holds a copy in the service and publishes it back to the queue when the delay is up.
The copy goes out under the queue's name on the default exchange, which addresses the queue of that name.
It carries [`RETRY_COUNT_HEADER`](api:ruststream::runtime::RETRY_COUNT_HEADER) raised by one.
The copy lives in the service's memory, so it is at-most-once over the delay: a process that stops before the timer fires loses it.
A [retry cap](./retries.md) counts these copies like immediate ones.

## Example

The first delivery finds the payment unsettled and asks for the refund again in 30 seconds. The copy comes back with the retry count, and the refund goes through.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/lapin_settlement_retry_after.rs#retry_after"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/lapin_settlement_retry_after.rs#retry_after"
```

</TabItem>
</Tabs>
