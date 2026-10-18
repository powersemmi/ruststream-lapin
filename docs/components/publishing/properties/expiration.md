---
sidebar_position: 2
title: expiration
description: "expiration gives one message, or every message of a policy, a TTL after which the broker drops or dead-letters it."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# expiration

[`expiration`](api:ruststream_lapin::LapinPublishSteps::expiration) gives one message a time to live; [`LapinPublish::expiration`](api:ruststream_lapin::LapinPublish::expiration) gives one to every message of a policy.

A message nobody consumed within its TTL leaves the queue.
The broker drops it, or dead-letters it where the queue has a [dead-letter exchange](../../settlement/dead-letter-exchange.md).
RabbitMQ expires a message when it reaches the head of its queue.
A policy that sets no TTL sends messages that do not expire.
AMQP counts the TTL in whole milliseconds, and a shorter non-zero TTL becomes one millisecond.
A delivery reports it under [`EXPIRATION_HEADER`](api:ruststream_lapin::EXPIRATION_HEADER) in milliseconds.

## Example

A quote is worth a minute and a flash quote ten seconds. The broker drops a quote nobody consumed in time.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/lapin_publishing_expiration.rs#expiration"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/lapin_publishing_expiration.rs#expiration"
```

</TabItem>
</Tabs>
