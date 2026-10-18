---
title: 拓扑
sidebar_position: 4
description: "决定由谁创建 RabbitMQ 的队列和交换机：用 declare_topology 交给服务，或交给运维；涵盖经典队列、仲裁队列和原始声明参数。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 拓扑

本指南描述服务的队列、交换机和绑定，并决定由谁创建它们：服务本身，或者运维 RabbitMQ 的人。

队列描述符说明服务期望在服务器上找到什么。
只有 Broker 通过 [`declare_topology(true)`](../components/broker/declare-topology.md) 开启声明，服务才会创建它们。
示例是一个支付服务：一个存放付款的复制队列，一个存放失败付款的队列，以及每个实例各自的一个汇率队列。

## 经典队列与仲裁队列 {/* #classic-and-quorum-queues */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/topology.rs#queues"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/topology.rs#queues"
```

</TabItem>
</Tabs>

RabbitMQ 有两种队列类型，类型在创建队列时就固定下来。
[经典队列](../components/subscriptions/rabbit-queue.md)位于一个节点上。
[仲裁队列](../components/subscriptions/rabbit-quorum-queue.md)通过 Raft 在集群中复制，丢失一个节点后依然存在。
每种类型有自己的描述符：[`RabbitQueue`](api:ruststream_lapin::RabbitQueue) 和 [`RabbitQuorumQueue`](api:ruststream_lapin::RabbitQuorumQueue)。
不能丢失的数据（例如付款）用仲裁队列，其余的用经典队列。
RabbitMQ 的[仲裁队列指南](https://www.rabbitmq.com/docs/quorum-queues)比较了这两种类型。

仲裁队列总是持久的、在连接之间共享的、不会自动删除的。
所以 `RabbitQuorumQueue` 没有 `durable`、`exclusive` 和 `auto_delete`，与自身类型相矛盾的仲裁队列无法通过编译。
仲裁队列还统计每条消息的投递次数，[重试指南](retries.md)正是基于这一点。

[`dead_letter_exchange`](../components/settlement/dead-letter-exchange.md) 为 `payments` 指定被拒绝消息的去向。
`charge` 丢弃的付款进入 `payments.dead` 交换机，再从那里进入与它绑定的 `payments.failed` 队列。
当该交换机按路由键路由时，[`dead_letter_routing_key`](api:ruststream_lapin::RabbitQueue::dead_letter_routing_key) 在这条路线上替换消息的路由键。

## 每个实例一个队列 {/* #a-queue-of-each-instance */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/topology.rs#instance"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/topology.rs#instance"
```

</TabItem>
</Tabs>

每个实例都需要全部汇率，所以每个实例读取自己的队列，该队列绑定到 `fx-rates` 这个 fanout 交换机。
队列名包含进程 id，所以两个实例永远不会共用它。
[`exclusive(true)`](api:ruststream_lapin::RabbitQueue::exclusive) 只允许当前连接使用这个队列，连接关闭时 RabbitMQ 删除它。
[`auto_delete(true)`](api:ruststream_lapin::RabbitQueue::auto_delete) 在最后一个消费者离开后删除它。
[`durable(false)`](api:ruststream_lapin::RabbitQueue::durable) 不让它写入磁盘：重启之后，汇率已经没有价值。
RabbitMQ 4 只接受排他的非持久队列，所以 `durable(false)` 要与 `exclusive(true)` 一起使用。

[`argument`](../components/subscriptions/arguments.md) 传递描述符没有对应方法的声明参数，这里是 `x-message-ttl`：一分钟内没人读取的汇率会过期。
`x-max-length`、`x-max-priority`、`x-overflow` 和 `x-single-active-consumer` 也用同样的方式设置。
参数值是 [`AMQPValue`](api:ruststream_lapin::AMQPValue)，[`arguments`](api:ruststream_lapin::RabbitQueue::arguments) 则接收一整张表。

## 由服务声明 {/* #declared-by-the-service */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/topology.rs#app"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/topology.rs#app"
```

</TabItem>
</Tabs>

开启 `declare_topology(true)` 后，每个订阅在启动时声明其描述符所说明的内容。
它先声明绑定中提到的交换机，再声明带参数的队列，最后声明绑定。
默认交换机和内置的 `amq.*` 交换机存在于每台服务器上，从不声明。
只要声明与服务器上已有的一致，重复声明就没有副作用，所以服务的每个实例都声明同样的队列。

与已有队列不一致的声明会失败。
以不同的持久性或不同的参数再次声明队列时，RabbitMQ 回应 `PRECONDITION_FAILED`，服务无法启动：[`AmqpError::Declare`](../components/broker/errors.md) 会指出是哪个队列。
要修改队列的类型和参数，只能删除后重新声明，队列里的消息会随之丢失，所以这类修改通过迁移完成，而不是通过部署。
RabbitMQ 在[属性等价性](https://www.rabbitmq.com/docs/queues#property-equivalence)一节中说明了哪些属性必须一致。

## 由运维负责 {/* #owned-by-operations */}

`DECLARE_TOPOLOGY=false` 以关闭声明的方式启动同一个服务。
这时描述符什么都不声明：它们说明服务期望的队列、参数和绑定，由运维来创建。
不存在的队列会让订阅在启动时返回 [`AmqpError::Subscribe`](../components/broker/errors.md)，所以缺少队列会中止部署，而不是丢失消息。

运维通常用 RabbitMQ 的[策略（policy）](https://www.rabbitmq.com/docs/policies)来设置长度上限和 TTL，而不是用声明参数。
策略无需重新声明就能修改队列，策略设置的参数也从不与服务的声明冲突。
这时描述符只需写明服务自身依赖的内容。

关闭声明还会改变仲裁队列的重试声明能做的事，[重试指南](retries.md)对此有说明。

## 测试 {/* #testing */}

示例文件中的测试分别在开启和关闭声明的情况下拒绝一笔付款，并在 `payments.failed` 中找到它。
进程内 RabbitMQ 遵循 Broker 的设置：它声明描述符所说明的内容，并拒绝服务器会拒绝的东西，例如非排他的非持久队列、默认交换机上的绑定，或以不同设置再次声明的队列。
关闭声明时，订阅提到的每个队列在进程内都已存在，所以缺少队列的情况要用[真实服务器上的测试](../components/testing/live.md)来检查。
[测试 RabbitMQ 服务](testing.md)讲解了测试套件。
