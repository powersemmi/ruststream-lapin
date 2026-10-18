---
title: 交换机与路由
sidebar_position: 3
description: "通过默认、topic、fanout 和 headers 交换机路由 RabbitMQ 消息：绑定写在队列上，交换机写在发布策略上。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 交换机与路由

本指南通过 RabbitMQ 交换机路由消息：按路由键、发给每个绑定的队列，以及按消息头。

发布者从不直接写入队列。
它带着路由键发布到交换机，交换机把消息复制到每个绑定相匹配的队列。
在 RustStream 中，队列的绑定写在它的[描述符](../components/subscriptions/rabbit-queue.md)上，消息发往哪个交换机则由[发布策略](../components/publishing/exchange.md)决定。
路由键就是消息的名字：消息类型上的 `#[outgoing(name = "..")]`，或者发布时给出的名字。

示例是一家网店：它在 topic 交换机上发布订单，在 fanout 交换机上发布价格变动，在 headers 交换机上发布发货信息。
Broker 声明交换机、队列和绑定，所以示例可以在空的 RabbitMQ 上运行（[拓扑](topology.md)）。

## 默认交换机 {/* #the-default-exchange */}

前面的每个服务都发布到默认交换机。
它是一个 direct 交换机，每个队列都以自己的名字绑定到它。
因此路由键为 `orders` 的消息到达 `orders` 队列，服务用 `#[subscriber("orders")]` 读取它，完全不需要绑定。
服务器和进程内 RabbitMQ 都拒绝在默认交换机上建立绑定。

你自己的 [direct 交换机](../components/exchanges/index.md)以同样的方式路由：一个绑定指定一个路由键，消息只到达以其路由键原样绑定的队列。

## topic 交换机 {/* #topic-exchanges */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/routing.rs#topic"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/routing.rs#topic"
```

</TabItem>
</Tabs>

`place_order` 把每个订单作为 `OrderPlaced` 发布，路由键为 `order.placed.<region>`。
该类型没有声明名字，所以发布时用 `.to(&key)` 指定。
[topic 交换机](../components/exchanges/topic.md)把路由键与每个绑定的模式逐词比较，词与词之间以点分隔。
`*` 恰好匹配一个词，`#` 匹配任意多个词，包括零个。

因此 `order.placed.*` 接收所有地区的订单，`*.*.eu` 只接收欧盟的订单，`#` 接收这个交换机上的所有消息。
来自美国的订单到达通知和审计，欧盟仓库永远看不到它。
处理器通过 [`Ctx<RoutingKey>`](../components/context/index.md) 读取消息到达时所带的路由键。

## fanout 交换机 {/* #fanout-exchanges */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/routing.rs#fanout"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/routing.rs#fanout"
```

</TabItem>
</Tabs>

[fanout 交换机](../components/exchanges/fanout.md)把每条消息复制到绑定它的每个队列，并忽略路由键。
`change_price` 以 `PriceChanged` 作为回复，店面和搜索索引各得到一份副本。
绑定的路由键为空，因为这个交换机不读取路由键。
新增一个价格变动的读者，只是多一个绑定到 `prices` 的队列；发布者不需要改动。

## headers 交换机 {/* #headers-exchanges */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/routing.rs#headers"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/routing.rs#headers"
```

</TabItem>
</Tabs>

headers 交换机按消息头而不是路由键来路由。
[`bind_with`](../components/exchanges/bind-with.md) 为绑定指定参数：要匹配的消息头，以及 `x-match`，它规定其中多少个必须匹配。
取 `all` 时，消息必须匹配全部消息头；取 `any` 时，匹配一个就够了。
`ship_order` 用 `with_headers` 为每次发货设置 `region` 和 `tier`。
欧盟的 gold 级发货到达两个队列，欧盟的 silver 级发货只到达保险队列，美国的 silver 级发货哪个队列也不到达。

普通的 `bind` 用于按路由键路由的交换机。
在 headers 交换机上，它建立的绑定没有要匹配的消息头，而对空集合取 `all` 对每条消息都成立。
[`FieldTable`](api:ruststream_lapin::FieldTable) 和 [`AMQPValue`](api:ruststream_lapin::AMQPValue) 来自 lapin，由 prelude 重新导出。

## 发布到交换机 {/* #publishing-to-an-exchange */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/routing.rs#app"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/routing.rs#app"
```

</TabItem>
</Tabs>

交换机只在挂载点的策略上写一次：[`Publish::default().exchange("events")`](api:ruststream_lapin::LapinPublish::exchange)。
处理器的函数体只写消息及其路由键，所以只要挂载点换一个交换机，同一个处理器就发布到那里。
回复在 `.out_reply(..)` 处接收同样的策略。

RabbitMQ 拒绝发布到不存在的交换机，并关闭发布所用的信道。
这里的订阅在启动时、在任何处理器发布之前声明 `events`、`prices` 和 `shipments`。
如果服务发布到一个它并不从中读取的交换机，就要依靠运维或另一个服务来声明这个交换机。

订阅的每个绑定也出现在服务的 AsyncAPI 文档中，位于其通道的 [`x-ruststream-amqp`](../components/asyncapi/extension.md) 扩展里。

## 一致性哈希 {/* #consistent-hashing */}

`plugin-consistent-hash` feature 加入 [`RabbitExchange::consistent_hash`](../components/exchanges/consistent-hash.md)，即 RabbitMQ 一致性哈希插件提供的交换机类型。
它按路由键的哈希把一个消息流分散到多个队列，每个队列以自己的权重作为路由键进行绑定。
路由键相同的消息总是到达同一个队列。
插件必须在服务器上启用，路由也只在服务器上发生，所以它的测试针对真实的 RabbitMQ 运行。

## 测试 {/* #testing */}

示例文件中的测试发布一个订单、一次价格变动和三个已支付订单，并检查每条消息到达了哪些队列。
进程内 RabbitMQ 按描述符声明的同一组绑定，通过默认、direct、topic、fanout 和 headers 交换机进行路由。
[测试 RabbitMQ 服务](testing.md)讲解了测试套件。
