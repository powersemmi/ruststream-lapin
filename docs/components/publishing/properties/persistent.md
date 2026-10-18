---
sidebar_position: 3
title: persistent
description: "persistent marks a message to survive a broker restart; policies default to persistent, and a request policy to transient."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# persistent

[`persistent`](api:ruststream_lapin::LapinPublishSteps::persistent) sets the delivery mode of one message; [`LapinPublish::persistent`](api:ruststream_lapin::LapinPublish::persistent) sets it for every message of a policy.

A persistent message (delivery mode 2) on a durable queue survives a broker restart.
A transient message (delivery mode 1) does not.
The publish policies default to persistent.
[`LapinRequest`](api:ruststream_lapin::LapinRequest) defaults to transient: a request nobody waits for after its timeout gains nothing from a restart.

## Example

Live meter readings are transient, because the next tick replaces them. The reading a bill is made from is persistent.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/lapin_publishing_persistent.rs#persistent"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/lapin_publishing_persistent.rs#persistent"
```

</TabItem>
</Tabs>
