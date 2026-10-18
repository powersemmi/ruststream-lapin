---
sidebar_position: 1
title: Server
description: "The server reports the host and the AMQP version; the credentials and the virtual host of the connection URI stay in the service."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Server

The server reports the host and the AMQP version; the credentials and the virtual host of the connection URI stay in the service.

A broker registered with [`with_broker_labeled`](api:ruststream::runtime::RustStream::with_broker_labeled) is a server of the document, under its label.
Its `host` is the host and port of the [connection URI](../broker/connection.md).
The user, the password, the virtual host and the query of the URI stay out of it.
A URI with no port gives the host alone.

The `protocol` is `amqp`, and `protocolVersion` is `0.9.1`.
AMQP 1.0 is a different protocol with a binding of its own, so the version tells a reader which one a client speaks here.

## Example

The broker connects as the user `inventory` to the virtual host `prod`.
Its server in the document is `rabbit:5672`, protocol `amqp`, version `0.9.1`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/lapin_asyncapi.rs#server"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/lapin_asyncapi.rs#server"
```

</TabItem>
</Tabs>
