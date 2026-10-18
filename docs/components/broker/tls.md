---
sidebar_position: 2
title: TLS
description: "An amqps URI with one of the tls-native-tls, tls-rustls or tls-rustls-ring features connects over TLS, through lapin's own backends."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# TLS

An `amqps` URI with one of the `tls-native-tls`, `tls-rustls` or `tls-rustls-ring` features connects over TLS, through lapin's own backends.

Each feature turns on one TLS backend of the `lapin` client:

| Feature | Backend |
| --- | --- |
| `tls-native-tls` | the platform's TLS library: OpenSSL on Linux, Secure Transport on macOS, SChannel on Windows |
| `tls-rustls` | rustls with the `aws-lc-rs` crypto provider, checking the server against the platform's trusted roots |
| `tls-rustls-ring` | rustls with the `ring` crypto provider |

A service enables one of them on its `ruststream-lapin` dependency, for example `features = ["tls-rustls"]`.
RabbitMQ listens for TLS on port 5671 by default, so an `amqps` URI usually names that port.
The URI keeps the form of a [plain connection](./connection.md): credentials, host, port and virtual host.

The handshake happens when the runtime connects the broker at startup.
A certificate the client does not trust fails startup with [`AmqpError::Connect`](api:ruststream_lapin::AmqpError::Connect).
The [in-process transport](../testing/index.md) opens no socket: it parses the `amqps` URI as the real connect does and runs the service's handlers without a handshake.

## Example

The service connects to `rabbit.example.com` over TLS, as the user `orders` on the virtual host `shop`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/lapin_broker_tls.rs#broker"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/lapin_broker_tls.rs#broker"
```

</TabItem>
</Tabs>
