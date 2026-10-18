---
title: LapinPublish
description: "LapinPublish, Publish in the prelude, sends fire-and-forget: the message name is the routing key, and a publish resolves once the frame is written."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# LapinPublish

[`LapinPublish`](api:ruststream_lapin::LapinPublish), `Publish` in the prelude, is the fire-and-forget publish policy: a publish resolves once the frame is written, with no broker confirm.

The message name is the routing key, and the exchange belongs to the policy.
By default that is the default exchange, where a routing key addresses the queue of the same name; [`exchange`](./exchange.md) names another.
The policy holds no connection.
At startup the runtime pairs it with the connected broker into a [`LapinPublisher`](api:ruststream_lapin::LapinPublisher), which publishes on the connection's shared publish channel.
A publisher kept past shutdown answers every publish with [`AmqpError::Closed`](api:ruststream_lapin::AmqpError::Closed).

A mount site names the policy wherever a registration publishes: `.out_reply(Publish::default())` for a reply, `.out(Slot, Publish::default())` for a slot, `.out_retry(Publish::default())` for retry copies.
It is also the broker's default policy, so a registration that names none replies and retries through it.
Where a reply goes is a property of the reply type, as the core's [replies page](https://powersemmi.github.io/ruststream/latest/components/subscribers/publishing/replies/) describes.
A publish outside a handler goes through [`after_startup`](api:ruststream::runtime::BrokerScope::after_startup) on the broker scope: once the subscriptions are open, the runtime pairs the policy and hands the hook the live publisher.

The policy holds the defaults of the per-message [properties](./properties/index.md).
[`confirms`](api:ruststream_lapin::LapinPublish::confirms) and [`server_tx`](api:ruststream_lapin::LapinPublish::server_tx) move to the [transactional policies](../transactions/index.md) and keep these settings.

[Automatic recovery](../broker/connection.md#automatic-recovery) restores publisher channels and confirms, but does not replay failed publishes.
A publish during an outage can return an error, and the outcome of an in-flight publish can be uncertain.
An application that retries must account for possible duplicates. Connection readiness alone does not mean every publisher channel has finished recovering.

## Example

Every order is answered with a confirmation, routed under the name the reply type declares. Once its subscriptions are open, the service announces itself.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/lapin_publishing_publish.rs#publish"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/lapin_publishing_publish.rs#publish"
```

</TabItem>
</Tabs>
