---
sidebar_position: 3
title: ConfirmsTransaction
description: "owned_transaction opens independent confirm transactions on one publisher; each settles on its own, and commit consumes it."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# ConfirmsTransaction

[`ConfirmsTransaction`](api:ruststream_lapin::ConfirmsTransaction) is an owned confirm transaction: a value that holds its own publish buffer.

A handler bounded `Out<impl OwnedTransactions, Slot>` opens one with `transaction()` on its slot, and every call opens another.
Any number can be open on one publisher, and settling one never touches another.
The publisher keeps publishing directly while they are open.
`commit` publishes the buffer in order and awaits every confirm, and `abort` discards it without touching the broker.
Both consume the value, so a double commit or a publish after settling does not compile.
A failed commit loses the buffer, and the delivery's redelivery is the way to recover.
Only [`ConfirmsPublish`](./confirms-publish.md) offers the owned kind.

## Example

The shipment and the audit record of an order go out in two transactions. An order out of stock aborts the shipment and still commits its audit record.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/lapin_transactions_owned.rs#owned"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/lapin_transactions_owned.rs#owned"
```

</TabItem>
</Tabs>
