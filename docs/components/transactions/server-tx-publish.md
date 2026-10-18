---
sidebar_position: 2
title: ServerTxPublish
description: "ServerTxPublish uses AMQP channel transactions: publishes become visible together at commit, paid for with a synchronous round trip."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# ServerTxPublish

[`ServerTxPublish`](api:ruststream_lapin::ServerTxPublish) is the publish policy backed by AMQP channel transactions: `tx.select`, `tx.commit` and `tx.rollback`.

[`LapinPublish::server_tx`](api:ruststream_lapin::LapinPublish::server_tx) reaches it with the same settings.
It pairs into a [`ServerTxPublisher`](api:ruststream_lapin::ServerTxPublisher) on a channel of its own.
Between `begin` and `commit` the messages wait on the broker inside the channel transaction, and they become visible together at commit.
`abort` rolls them back on the server.
Each commit is a synchronous round trip, which makes the policy slower than [`ConfirmsPublish`](./confirms-publish.md).
Outside a transaction a publish behaves like a [fire-and-forget](../publishing/index.md) one.
The transaction is the channel's state, so the policy offers only the borrowed kind of the [transaction scope](./index.md).

## Example

The debit and the credit of a transfer are published in one server transaction. A frozen account rolls it back, and the debit already sent is discarded on the broker.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/lapin_transactions_server_tx.rs#ledger"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/lapin_transactions_server_tx.rs#ledger"
```

</TabItem>
</Tabs>
