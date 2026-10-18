---
title: AMQP bindings
description: "With asyncapi the generated document speaks AMQP 0.9.1: the server, the queue behind each channel and the properties of each send."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# AMQP bindings

With the `asyncapi` feature the generated document speaks AMQP 0.9.1: the server, the queue behind each channel and the properties of each send.

The core builds the AsyncAPI document from the app, before anything connects.
This crate adds the specification's `amqp` binding, version `0.3.0`, to the parts it knows about.
The [server](./server.md) reports the host and the AMQP version.
A [channel](./channels.md) reports the queue behind a subscription, or the exchange a publish leaves through.
An [operation](./operations.md) reports how a receive settles and which properties a send carries.
The exchanges a queue is bound to ride the crate's own [extension](./extension.md), `x-ruststream-amqp`.

Everything comes from the service's own declarations: the broker, the queue descriptors and the publish policies.
The document is published and shared, so no binding carries a credential.
The binary `#[ruststream::app]` writes prints the document with `asyncapi gen`.

## Example

The dependency line:

```toml
ruststream-lapin = { version = "0.7", features = ["asyncapi"] }
```

The service registers its broker under the label `rabbit`, the key of its server in the document.
`cargo run --example lapin_asyncapi --features asyncapi -- asyncapi gen` prints the document.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/lapin_asyncapi.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/lapin_asyncapi.rs#app"
```

</TabItem>
</Tabs>
