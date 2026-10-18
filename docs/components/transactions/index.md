---
title: Transactions
description: "A handler bounded Out<impl TransactionalPublisher> begins a scope, publishes and commits; the mount site picks confirms or a server transaction."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Transactions

A handler bounded `Out<impl TransactionalPublisher>` opens a transaction scope with `begin`, publishes through it, and ends it with `commit` or `abort`.

The scope's publishes become visible together at `commit`, and `abort` discards them.
The handler names the capability, [`TransactionalPublisher`](api:ruststream::TransactionalPublisher), and the mount site names the policy that provides it.
[`ConfirmsPublish`](./confirms-publish.md), `TransactionalPublish` in the prelude, buffers in the client and commits by awaiting a broker confirm for every message.
[`ServerTxPublish`](./server-tx-publish.md) runs an AMQP channel transaction on the server.
[`LapinPublish`](api:ruststream_lapin::LapinPublish) pairs into a publisher with no transaction surface, so a transactional handler mounted with it does not compile.

A publisher runs one such transaction at a time.
A second `begin` while one is open, or a `commit` or `abort` with none open, fails with [`AmqpError::Transaction`](api:ruststream_lapin::AmqpError::Transaction) and leaves the open transaction as it was.
A publisher is a shared handle: every clone uses one channel and one transaction state.
Several independent groups of messages in one handler take the owned kind, [`ConfirmsTransaction`](./confirms-transaction.md).

## Example

An order ships one message per item, in one transaction. An item without a name aborts the transaction, and the shipments already published are discarded.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/lapin_transactions_scope.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/lapin_transactions_scope.rs#handler"
```

</TabItem>
</Tabs>

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/lapin_transactions_scope.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/lapin_transactions_scope.rs#app"
```

</TabItem>
</Tabs>
