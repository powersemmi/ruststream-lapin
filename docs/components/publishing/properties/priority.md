---
sidebar_position: 1
title: priority
description: "priority sets the AMQP priority of one message or of every message of a policy; it orders deliveries only on an x-max-priority queue."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# priority

[`priority`](api:ruststream_lapin::LapinPublishSteps::priority) sets the AMQP `priority` of one message; [`LapinPublish::priority`](api:ruststream_lapin::LapinPublish::priority) sets it for every message of a policy.

A policy that sets no priority sends none.
RabbitMQ orders deliveries by priority only on a queue declared with `x-max-priority`, and a higher priority counts as that maximum.
On any other queue the priority travels to the consumer and orders nothing.
A delivery reports it under [`PRIORITY_HEADER`](api:ruststream_lapin::PRIORITY_HEADER) as decimal digits, and the handler reads it like any other header.
A delayed redelivery keeps the priority its original delivery reported.

## Example

A reading over 90 degrees raises an alert at priority 9, a warm one at the policy's 1. The alerts queue orders by priority, and its consumer pages someone for the urgent ones.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/lapin_publishing_priority.rs#priority"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/lapin_publishing_priority.rs#priority"
```

</TabItem>
</Tabs>
