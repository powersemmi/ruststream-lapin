---
title: Properties
description: "A handler bounds its slot on LapinPublishOptions and adjusts one message's AMQP properties with builder steps over the policy's defaults."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Properties

[`LapinPublishSteps`](api:ruststream_lapin::LapinPublishSteps) adds `priority`, `expiration` and `persistent` to the publish builder, so a handler sets an AMQP property of one message.

A publish policy holds the defaults for every message it sends, and a step adjusts one message over them.
[`LapinPublishOptions`](api:ruststream_lapin::LapinPublishOptions) holds the three settings, each optional: what a call leaves alone keeps the policy's value.
The steps exist on a publisher whose options type is `LapinPublishOptions`.
So the handler bounds its slot `Out<impl Publisher<Options = LapinPublishOptions>, Slot>` and still names no publisher type.
The steps come from this crate's prelude, the one prelude a handler body imports besides the framework's.
The values reach the frame as protocol fields, never as headers.
The publish still leaves through the slot's own entry, with its codec and transforms.
A test reads the options back with [`with_options`](api:ruststream::testing::PublishedAssertions::with_options) on `tb.out::<Slot>()`.

## Example

An expedited order leaves at priority 9 and expires after an hour. An ordinary order takes the policy's priority 3.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/lapin_publishing_properties.rs#steps"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/lapin_publishing_properties.rs#steps"
```

</TabItem>
</Tabs>
