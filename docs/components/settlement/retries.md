---
sidebar_position: 2
title: Retries
description: "On a classic queue max_attempts and dead_letter are the runtime's: it counts the copies it publishes back to the queue by name."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Retries

[`max_attempts(n)`](api:ruststream::runtime::Mounting::max_attempts) and [`dead_letter(name)`](api:ruststream::runtime::Mounting::dead_letter) cap the deliveries of one message; on a classic queue the runtime keeps the count.

`max_attempts(n)` is how many deliveries one message gets, the first included.
`dead_letter(name)` is where it goes once they run out.
A classic queue counts nothing, so under a cap every retry, an immediate one too, travels as a copy.
The runtime settles the delivery and publishes a copy of the message back to the queue.
The copy carries [`RETRY_COUNT_HEADER`](api:ruststream::runtime::RETRY_COUNT_HEADER) (`x-ruststream-retry-count`) raised by one, and the handler reads it like any other header.
The copy goes back under the queue's own name: on the default exchange a routing key addresses the queue of that name.
The delivery that reaches the cap is published under `name` instead, so `name` is the queue the message lands in, with its count.
A bare queue name, `#[subscriber("payments")]`, takes the same cap.

The copies leave through the broker's default publish policy.
[`out_retry(policy)`](api:ruststream::runtime::Mounting::out_retry) names another one, and the position takes a slot's steps: `.codec(..)`, `.transform(..)`, `.to(name)`.
The example names the confirms policy, whose publish resolves once the broker has confirmed the copy.
A retry policy aimed at a topic or direct exchange reaches the queue only through a binding under the queue's name.
On a quorum queue the same two steps become the queue's own [delivery limit](./delivery-limit.md).

## Example

A declined card is retried at once. The third delivery is the last: the payment goes to `payments.dead`, where a second subscription reads the count.

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
