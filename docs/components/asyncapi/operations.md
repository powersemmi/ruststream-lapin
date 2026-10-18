---
sidebar_position: 3
title: Operations
description: "A receive operation reports manual acknowledgement; a send reports the delivery mode, priority and expiration its policy fixes."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Operations

A receive operation reports manual acknowledgement; a send reports the delivery mode, priority and expiration its policy fixes.

Every receive operation carries `ack: true`.
This crate acknowledges each delivery by hand, so the handler's outcome settles it.

A slot entry and a dead-letter destination each have a send operation.
Its binding carries the properties every message through the policy takes.
`deliveryMode` is `2`, persistent, unless the policy sets [`persistent(false)`](../publishing/properties/persistent.md), which gives `1`.
`priority` and `expiration`, in milliseconds, appear when the policy fixes a [priority](../publishing/properties/priority.md) or an [expiration](../publishing/properties/expiration.md).
The document describes the declaration, so a step that adjusts one message leaves it as it is.

A reply is the `reply` of its receive operation.
Under [`DirectReplyTo`](../request/direct-reply-to.md) the reply's `address` has the `location` `$message.header#/reply-to`: a client reads the address from the request's `reply-to` header.

## Example

The operation `receive_inventory_check` reports `ack: true` and a reply addressed by `reply-to`.
The operation `send_inventory_events_inventory_restock` reports delivery mode `2`, priority `4` and an expiration of `30000`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/lapin_asyncapi.rs#operations"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/lapin_asyncapi.rs#operations"
```

</TabItem>
</Tabs>
