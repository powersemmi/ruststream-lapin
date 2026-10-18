---
sidebar_position: 1
title: DirectReplyTo
description: "DirectReplyTo sends each reply to the address the request named and echoes its correlation id, over a reply type with no destination."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# DirectReplyTo

[`DirectReplyTo`](api:ruststream_lapin::DirectReplyTo) is a reply transform: it sends each reply to the address the request named in `reply-to` and echoes the request's `correlation-id`.

It mounts on the reply position: `.out_reply(policy).transform(DirectReplyTo)`.
The handler is an ordinary reply handler and knows nothing about the requester.
The transform names the destination of every reply, so it mounts only over a reply type that declares none.
Over a type with its own `#[outgoing(name = "..")]` the mount does not compile.
The name in the mount site's `reply("..")` is the fallback: a request without `reply-to` is answered there, and the generated AsyncAPI document reports it.
A [`LapinRequest`](./index.md) requester and a requester on another stack both name the reply address this way.

## Example

The inventory service answers every stock check at the address the request named. A request without one is answered at `inventory.check.unrouted`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/lapin_request_direct_reply_to.rs#responder"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/lapin_request_direct_reply_to.rs#responder"
```

</TabItem>
</Tabs>
