---
sidebar_position: 1
title: Connection
description: "The URI carries credentials, virtual host and scheme; connection_name labels the service in the management UI, over one connection."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Connection

The URI carries the credentials, the virtual host and the scheme; [`connection_name`](api:ruststream_lapin::LapinBroker::connection_name) labels the service in the management UI.

The URI has the form `amqp://user:password@host:port/vhost`.
A URI without credentials logs in as `guest`, and RabbitMQ admits `guest` only from `localhost`.
A URI without a virtual host connects to the default one, `/`.
The `amqps` scheme opens the connection over [TLS](./tls.md).
A URI that does not parse fails startup with [`AmqpError::Connect`](api:ruststream_lapin::AmqpError::Connect), in a test as on a server.

`connection_name` is the name the management UI lists for the connection.
A connection without one is listed by its client address and port.

One connection carries the whole service.
Each subscription opens a channel of its own on it.
Fire-and-forget publishes share one channel.
A [confirms](../transactions/confirms-publish.md) or [server-transaction](../transactions/server-tx-publish.md) publisher opens its own channel on first use, because both modes are channel state.

## Example

The service logs in as `orders` on the virtual host `shop`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/lapin_broker_connection.rs#broker"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/lapin_broker_connection.rs#broker"
```

</TabItem>
</Tabs>
