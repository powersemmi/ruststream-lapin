---
sidebar_position: 2
title: Headers
description: "content-type, correlation-id, reply-to and message-id ride their AMQP properties; every other header lands in the header table."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Headers

A publish's headers travel in the AMQP frame: `content-type`, `correlation-id`, `reply-to` and `message-id` ride the native properties of those names, and every other header lands in the header table.

A handler hands the headers to the publish builder with `.with_headers(..)`, as a [`HeaderMap`](api:ruststream::HeaderMap) or as the typed contract its message declares.
The four identity headers become properties, so a consumer written against another client finds them where the protocol puts them.
Every other header is a byte string in the header table, so a binary value survives the round trip.
A delivery reports both kinds back as headers, and the handler reads them from its context.
A header another client or the server wrote with a typed value arrives too: a boolean as `true` or `false`, an integer or a timestamp in decimal, a float as its shortest decimal, an AMQP decimal with its scale applied (`19.99`), and an array or a table as compact JSON with a table's fields in name order.
So the server's `x-death` record reads as `[{"count":1,"exchange":"","queue":"orders","reason":"rejected",...}]`.
The text keeps the value and not its AMQP type, so a header this service publishes again travels as a string.
A delivery also reports two properties under headers of their own: the priority under [`PRIORITY_HEADER`](api:ruststream_lapin::PRIORITY_HEADER) (`amqp-priority`) and the TTL under [`EXPIRATION_HEADER`](api:ruststream_lapin::EXPIRATION_HEADER) (`amqp-expiration`).
A header never sets a delivery property: priority, expiration and delivery mode are [properties](./properties/index.md) of the policy and the publish builder.
An AMQP property is a short string of at most 255 bytes, and a longer identity header fails the publish with [`AmqpError::InvalidOptions`](api:ruststream_lapin::AmqpError::InvalidOptions) before anything is routed.

## Example

The invoice carries the order's reference as its `message-id` property and the tenant in the header table. The billing side reads both from its context.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/lapin_publishing_headers.rs#headers"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/lapin_publishing_headers.rs#headers"
```

</TabItem>
</Tabs>
