---
sidebar_position: 4
title: retry_after
description: "retry_after redelivers no sooner than a delay through the queue's waiting queue; a queue without one requeues at once."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# retry_after

[`retry_after(delay)`](api:ruststream::runtime::HandlerOutcome::retry_after) asks for a redelivery no sooner than `delay`, for a message that is not ready yet.

AMQP has no per-message delay, so the queue takes one with [`delay`](./delay.md).
On a `retry_after`, the crate publishes a copy to the queue's waiting queue and acknowledges the delivery.
The waiting queue releases the copy back to the queue once the delay is up.
The copy carries [`RETRY_COUNT_HEADER`](api:ruststream::runtime::RETRY_COUNT_HEADER) raised by one, and it lives on the broker, so a service that stops during the delay does not lose it.
On a queue without a `delay`, `retry_after` requeues the delivery at once and drops the delay; the runtime logs a warning.

## Example

The first delivery finds the payment unsettled and asks for the refund again in 30 seconds. The waiting queue `refunds.retry` holds the copy, which comes back with the retry count, and the refund goes through.

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
