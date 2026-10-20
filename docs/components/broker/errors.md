---
sidebar_position: 6
title: Errors
description: "AmqpError is the crate's one error: connect, publish, subscribe, consume, declare, request, timeout, closed, transaction and options."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Errors

[`AmqpError`](api:ruststream_lapin::AmqpError) is the crate's one error: connect, publish, subscribe, consume, declare, request, timeout, closed, transaction and options.

Each variant names the step that failed.
The `lapin` error behind it is its `source`, so the client library's types stay out of the crate's API.

| Variant | When |
|---|---|
| `Connect` | the URI did not parse, the server was unreachable or refused the login, or the close failed |
| `Publish` | the server refused a message, such as a body over its 16 MiB `max_message_size`, or did not confirm it |
| `Subscribe` | a channel, its prefetch or its consumer could not be opened, such as on a queue the server does not have |
| `Consume` | an open consumer failed to receive |
| `Declare` | the server refused an exchange, a queue or a binding the service declares |
| `Request` | a request could not be sent, or its reply could not be received |
| `RequestTimeout` | no reply arrived within the requester's deadline |
| `Closed` | a publisher or a requester was used after the [shutdown](./shutdown.md); it names the target |
| `Transaction` | a transaction call came out of order: a commit with none open, or a second begin |
| `InvalidOptions` | the settings cannot work together, such as half of a quorum queue's retry declaration |

A service meets the error in two places.
A failed startup carries it as the source of the runtime's error, and the service exits before it consumes anything.
A publish from a handler returns it inside the core's [`PublishError`](api:ruststream::runtime::PublishError), and the handler decides how the delivery settles.
The enum is `#[non_exhaustive]`, so a `match` on it ends with a wildcard arm.

With [automatic recovery](./connection.md#automatic-recovery) enabled, transient disconnects pause consumers.
When retries run out, a consumer stream yields a terminal `AmqpError::Consume` and ends.
Pairing `ServerTxPublish` with `auto_recover(true)` fails with `AmqpError::InvalidOptions`; direct server-transaction operations reject that combination too.

## Example

A shipment the server refuses is logged with its cause, and the order is dropped to the queue's dead-letter exchange.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/lapin_broker_errors.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/lapin_broker_errors.rs#handler"
```

</TabItem>
</Tabs>
