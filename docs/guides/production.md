---
sidebar_position: 9
description: "Run a RabbitMQ service in production: the connection URI, TLS, a named connection, prefetch, channels, connection loss and shutdown."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Running in production

This guide configures the connection of a RabbitMQ service for production and describes what it does when the connection or the service goes away.

## The connection

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/production.rs#broker"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/production.rs#broker"
```

</TabItem>
</Tabs>

The [connection URI](../components/broker/connection.md) carries everything about the connection: the scheme, the credentials, the host, the port and the virtual host.
`amqp://orders:secret@rabbit.internal:5672/shop` logs in as `orders` to the virtual host `shop`.
The default virtual host `/` is written `%2f`.
RabbitMQ's [URI specification](https://www.rabbitmq.com/docs/uri-spec) lists the query parameters, such as `heartbeat`.
The service reads the URI from `AMQP_URL`, so the secret stays out of the code.

[`connection_name`](api:ruststream_lapin::LapinBroker::connection_name) labels the connection in the management UI, so an operator sees which service and version holds it.
[`prefetch`](../components/subscriptions/prefetch.md) caps the unacknowledged deliveries of each subscription, which is how a slow service pushes back on the server ([Work queues](work-queues.md)).

`LapinBroker::new` only records the URI.
The service connects at startup, and a URI it cannot parse or a server it cannot reach stops the start with [`AmqpError::Connect`](../components/broker/errors.md).

## TLS

An `amqps://` URI connects over TLS, usually on port 5671.
TLS comes from one of three features of `ruststream-lapin`, each mapped onto lapin's own:

| Feature | TLS library |
|---|---|
| `tls-native-tls` | the platform's: OpenSSL on Linux, Secure Transport on macOS, SChannel on Windows |
| `tls-rustls` | rustls |
| `tls-rustls-ring` | rustls with the `ring` crypto provider |

The server's certificate is checked against the trusted roots of the chosen library.
RabbitMQ's [TLS guide](https://www.rabbitmq.com/docs/ssl) covers the server side.
The [TLS component](../components/broker/tls.md) shows the dependency lines.

The service's AsyncAPI document reports the host and the AMQP version, never the credentials or the virtual host of the URI: the document is published and shared.

## Topology in production

A production service usually runs with `declare_topology` off, against queues that operations create and change.
A queue that does not exist then fails the start instead of losing messages ([Topology](topology.md)).
A quorum queue's attempt cap and a delayed retry's waiting queue are the server's in that setup too ([Acknowledgements, retries and dead letters](retries.md)).

## Connections and channels

One connection carries the whole service.
Each subscription opens a channel of its own, so an error on one queue does not stop the others.
Fire-and-forget publishes share one channel.
A confirms publisher, a channel-transaction publisher and a requester each open a channel of their own on first use, because confirms, transactions and direct reply-to are the state of a channel.

RabbitMQ closes a channel on a channel-level error, such as a publish to an exchange that does not exist.
The connection stays up, and a publisher opens a new channel on its next call.

## Connection loss

Connection recovery is disabled by default. Enable `LapinBroker::auto_recover(true)` to let Lapin reconnect with exponential backoff, including the initial connection.
The default limit is 16 retries; `.recovery_retries(n)` changes it, and zero disables retries.
Lapin restores channels, declared exchanges, queues, bindings, prefetch, publisher confirms and consumers through the existing handles. It does not restart the application or rerun startup hooks.

Configure a heartbeat in the URI, such as `amqp://localhost:5672?heartbeat=30`, to detect silent connection loss.
`ConnectedLapinBroker::is_connected()` checks the AMQP connection for a readiness endpoint: false during disconnection or after shutdown.
A true result does not mean channel and topology recovery has finished or that workers are healthy; it is separate from the service's runtime health.

Failed publishes are not replayed, and deadlines of RPC requests in flight keep running.
`ServerTxPublish` requires `auto_recover(false)` because transaction state is lost on reconnect.
If retries run out at startup, connection fails; after startup, the consumer stream yields a terminal consume error and ends.

Unsettled deliveries in queues that survive the disconnect return to RabbitMQ and can be redelivered, marked [`Redelivered`](../components/context/index.md).
Handlers must tolerate duplicates. A consumer cancelled by the server on a live channel, such as after its queue is deleted, ends its subscription.

## Shutdown

`SIGTERM` or <kbd>Ctrl</kbd>+<kbd>C</kbd> stops the service.
The runtime stops the subscriptions and closes the connection with a clean handshake.
RabbitMQ returns every delivery the service took and did not settle to its queue, so another instance takes it.
A publisher or a requester that outlives the connection, for example one held by a background task, returns [`AmqpError::Closed`](../components/broker/shutdown.md) instead of sending into a closed connection.

If shutdown begins during reconnect, waiting consumers are woken and broker shutdown does not wait for RabbitMQ to return. Existing handles are closed immediately; a later recovered connection is closed in the background, or cleanup ends when retries run out.

## Testing

The tests in the example file start the service from an `amqps://` URI with credentials and from a malformed one.
In process, the URI is parsed as a connection parses it, so a URI the service could not connect with does not start a test either.
TLS and connection loss need a server, so they belong to a [live test](../components/testing/live.md).
