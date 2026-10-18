---
title: 可靠发布
sidebar_position: 6
description: "以消息所需的保证发布到 RabbitMQ：发后即忘、发布者确认、信道事务，以及单条消息的优先级和 TTL。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 可靠发布

本指南选择服务要在多大程度上确信 RabbitMQ 已经收下它的消息，并设置单条消息的属性。

保证是挂载点上[发布策略](../components/publishing/index.md)的属性，而不是每次调用时的标志：

| 策略 | prelude 中的名字 | 发布何时完成 |
|---|---|---|
| [`LapinPublish`](api:ruststream_lapin::LapinPublish) | `Publish` | 帧写入连接时 |
| [`ConfirmsPublish`](../components/transactions/confirms-publish.md) | `TransactionalPublish` | RabbitMQ 确认消息时 |
| [`ServerTxPublish`](../components/transactions/server-tx-publish.md) | `ServerTxPublish` | 信道事务提交时 |

示例是一个订单服务。
它把一个订单拆成每个包裹一次发货，要么全部发出，要么一个也不发，并为每次发货预约快递员，加急的排在其他发货前面。

## 带确认的事务 {/* #confirmed-transactions */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/publishing.rs#confirms"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/publishing.rs#confirms"
```

</TabItem>
</Tabs>

`pack_order` 把它的槽位约束为 `Out<impl TransactionalPublisher>`，所以它只能挂载在支持事务的策略上。
`begin` 打开一个作用域，在其中发布的每次发货都先在服务里等待。
`commit` 发布它们，并在 RabbitMQ 确认了每一条之后完成。
没有描述的包裹会中止这个作用域，该订单的任何发货都不会发出。
提交失败时返回错误，订单可以再次投递、再次打包。

RabbitMQ 在承担起一条消息的责任时确认它：对持久队列上的持久化消息，是写入磁盘之后；对仲裁队列，是多数副本接受之后。
RabbitMQ 的[确认指南](https://www.rabbitmq.com/docs/confirms)说明了每条消息在什么时候被确认。

确认并不表示有队列收到了这条消息。
本 crate 发布时不带 `mandatory` 标志，所以对于不匹配任何绑定的消息，RabbitMQ 也会确认，然后丢弃它。
因此，带确认的发布要发往绑定已经存在的交换机，或者已经存在的队列：要么随服务一起声明它们（[拓扑](topology.md)），要么在由运维负责它们的地方检查它们。

## 信道事务 {/* #channel-transactions */}

[`ServerTxPublish`](../components/transactions/server-tx-publish.md) 通过 `Publish::default().server_tx()` 获得，它改用 AMQP 信道事务。
一个事务中的消息在 `commit` 时一起变得可见，`commit` 是一次与服务器的同步往返。
发布者确认更快，能覆盖大多数情况。
当消费者绝不能看到一组消息的一半时，例如一笔转账的两边，就用信道事务。

约束为 `Out<impl TransactionalPublisher>` 的处理器无需改动就能挂载在任一策略上。
用 `owned_transaction` 打开的 [`ConfirmsTransaction`](../components/transactions/confirms-transaction.md) 拥有自己的缓冲区，而不是借用发布者：可以同时打开多个，各自独立提交。
信道事务只提供借用式的作用域，因为事务就是信道本身的状态。

## 单条消息的属性 {/* #per-message-properties */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/publishing.rs#properties"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/publishing.rs#properties"
```

</TabItem>
</Tabs>

AMQP 为每条消息提供三个专属属性：[优先级](../components/publishing/properties/priority.md)、[过期时间](../components/publishing/properties/expiration.md)和[投递模式](../components/publishing/properties/persistent.md)。
策略设置经由它发布的每条消息都带有的值，这里是优先级 3。
发布构建器的[步骤](../components/publishing/properties/index.md)为单条消息设置它们：加急取件以优先级 9 发出，一小时内无人读取就过期。
槽位约束在 [`LapinPublishOptions`](api:ruststream_lapin::LapinPublishOptions) 上，所以处理器不必写出发布者类型；这些步骤来自本 crate 的 prelude。

这三个属性都不以消息头的形式传递：每个都是 AMQP 属性，RabbitMQ 在那里读取它们。
投递以消息头的形式把它们报告回来，键为 [`PRIORITY_HEADER`](api:ruststream_lapin::PRIORITY_HEADER) 和 [`EXPIRATION_HEADER`](api:ruststream_lapin::EXPIRATION_HEADER)。

优先级只在声明了 `x-max-priority` 的队列上影响投递顺序；在其他队列上，RabbitMQ 忽略它。
快递员的 `pickups` 队列由它的所有者带着这个[参数](../components/subscriptions/arguments.md)声明。
除非用 `persistent(false)` 另行指定，每个策略都发布持久化消息，所以持久队列上的消息在 Broker 重启后依然存在。

## 挂载点 {/* #the-mount-site */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/publishing.rs#app"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/publishing.rs#app"
```

</TabItem>
</Tabs>

`TransactionalPublish::default()` 就是 [`ConfirmsPublish`](api:ruststream_lapin::ConfirmsPublish)。
`Publish::default().priority(3)` 是为每条消息设置优先级的普通策略。
`.confirms()` 和 `.server_tx()` 把普通策略升级为更强的策略，并保留它的设置，例如它的交换机。
发后即忘的发布共用连接上的一个信道；发布者确认模式或事务模式的发布者各自打开自己的信道，因为这两种模式都是信道状态。

服务也可以在处理器之外发布，例如在 HTTP 端点中，所用的发布者由应用在启动时提供（[`LapinPublish`](../components/publishing/index.md)）。

## 测试 {/* #testing */}

示例文件中的测试检查：一个订单会发出每个包裹，一个有问题的包裹会让整单一个也不发出，以及每次取件带有哪些属性（用 `tb.out::<Pickups>()` 读取）。
进程内 RabbitMQ 把事务缓存到提交为止，并像服务器那样为每条消息组帧。
确认的含义和信道事务的原子性由服务器实现，所以要用[真实服务器上的测试](../components/testing/live.md)来检查。
[测试 RabbitMQ 服务](testing.md)讲解了测试套件。
