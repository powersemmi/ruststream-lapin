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

## Automatic recovery

Recovery is off by default. Enable it with [`auto_recover(true)`](api:ruststream_lapin::LapinBroker::auto_recover).
Lapin restores the connection, channels, declared exchanges, queues, bindings, prefetch, publisher confirms and consumers.
Existing handles stay usable; the application and its startup hooks do not restart.

Retries use Lapin's exponential backoff. The default limit is 16 retries, configurable with
[`recovery_retries(n)`](api:ruststream_lapin::LapinBroker::recovery_retries).
The limit applies to the initial connection and later reconnects. Zero allows an attempt without retrying;
a successful connection resets the retry budget. With recovery off, the initial connection is attempted once.
Exhausting startup retries returns `AmqpError::Connect`; exhausting recovery ends the affected consumer stream with `AmqpError::Consume`.

Set a heartbeat in the URI, such as `?heartbeat=30`, so RabbitMQ and Lapin can detect silent connection loss.
[`ConnectedLapinBroker::is_connected()`](api:ruststream_lapin::ConnectedLapinBroker::is_connected) is a synchronous
connection check for readiness: it is false while disconnected or after shutdown.
A true result does not guarantee that channel and topology restoration has finished, or that workers are healthy.

Lapin can restore a publisher channel before another channel has recreated an exclusive queue and its binding.
A publish in that window can be confirmed but unroutable: RabbitMQ discards it, so it is neither queued nor redelivered.
Publisher confirms alone do not prove that a route exists. Keep production topology provisioned independently of the reconnecting connection,
or establish that the route is ready before sending application messages.
This cross-channel recovery ordering is tracked in [Lapin issue #481](https://github.com/amqp-rs/lapin/issues/481).

A failed [publish](../publishing/index.md) is not replayed, outstanding [RPC deadlines](../request/direct-reply-to.md)
keep running, and [server transactions](../transactions/server-tx-publish.md) require recovery to remain disabled.

## Example

The service logs in as `orders` on the virtual host `shop`, enables recovery with 32 retries, and requests a 30-second heartbeat.

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
