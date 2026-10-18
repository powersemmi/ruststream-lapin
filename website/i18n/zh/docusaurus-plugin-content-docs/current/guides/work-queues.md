---
title: 工作队列
sidebar_position: 2
description: "在多个工作者之间共享一个 RabbitMQ 队列：竞争消费者、预取、按键保序的工作分区、批和单活跃消费者。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 工作队列

本指南让服务的多个工作者共享一个 RabbitMQ 队列，并在需要的地方保持顺序。

示例是一个发票服务。
每个实例读取同一个 `invoices` 队列，并渲染它取到的发票。
第二个队列按批把发票送进归档，第三个队列严格按队列顺序把发票送进账簿。

## 竞争消费者 {/* #competing-consumers */}

服务的每个实例都订阅同一个队列。
RabbitMQ 把每条消息轮流交给其中一个实例，从不同时交给两个。
多启动一个实例就多一个消费者，队列也就清空得更快。
某个实例取走而没有结算的消息，会在它的连接关闭时回到队列，由另一个实例取走。

服务不需要为此做任何事：每个实例的 `#[subscriber]` 里写的是同一个队列名。

## 预取 {/* #prefetch */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/work_queues.rs#app"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/work_queues.rs#app"
```

</TabItem>
</Tabs>

[`prefetch`](../components/subscriptions/prefetch.md) 限制一个订阅可以持有的未确认投递数，这里是 32。
消费者持有这么多投递时，RabbitMQ 不再向它发送任何消息。
因此慢的实例会停止接活，下一条消息交给还有余量的实例。
不设预取时，RabbitMQ 以网络允许的最快速度把队列推给消费者，慢的实例会积压本可以由其他实例处理的消息。
数量的类型是 `NonZeroU16`：不设置就表示不限制。
Broker 上的值适用于每个订阅，队列描述符可以为自己的队列覆盖它。

预取窗口由服务器维护，所以只有针对运行中的 RabbitMQ 的测试才能观察到它。

## 工作分区与按键保序 {/* #lanes-and-per-key-order */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/work_queues.rs#lanes"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/work_queues.rs#lanes"
```

</TabItem>
</Tabs>

`workers(8, by_key)` 让一个实例在八个[工作分区](../components/subscriptions/lanes.md)上同时处理最多八张发票。
发布发票的计费服务把客户放在 `amqp-partition-key` 消息头里（[`PARTITION_KEY_HEADER`](api:ruststream_lapin::PARTITION_KEY_HEADER)）。
键相同的发票进入同一个工作分区，按实例收到它们的顺序逐张处理。
不同客户的发票并行处理。
没有这个消息头的发票轮流进入下一个工作分区。
RabbitMQ 不读取这个消息头：工作分区是服务自己的约定。

顺序只在一个实例内部成立。
两个实例各自取走一个客户的一部分发票，所以它们的工作分区不能跨实例保序。
处理器读取 [`Redelivered`](../components/context/index.md)，在日志中记录 RabbitMQ 之前是否投递过这张发票。

## 批 {/* #batches */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/work_queues.rs#batches"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/work_queues.rs#batches"
```

</TabItem>
</Tabs>

切片参数表示接收一[批](../components/subscriptions/batches.md)消息，挂载点用 `.batch(nonzero!(64))` 指定批的大小。
AMQP 一次投递一条消息，所以批在服务里组装。
批在装满 64 张发票时，或者在第一张发票到达后经过 [`batch_wait`](api:ruststream_lapin::RabbitQueue::batch_wait) 时关闭；等待时间默认为 50 ms。
一批只包含 RabbitMQ 已经发来的消息，所以归档队列把自己的预取值提高到 64。
如果沿用 Broker 的 32，一批永远到不了 64。
处理器的结果结算这一批中的每张发票。

## 同一时刻只有一个消费者 {/* #one-consumer-at-a-time */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/work_queues.rs#single_active"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/work_queues.rs#single_active"
```

</TabItem>
</Tabs>

账簿需要所有实例上的每张发票都按队列顺序到达。
RabbitMQ 的[单活跃消费者](https://www.rabbitmq.com/docs/consumers#single-active-consumer)可以做到这一点：队列只向一个消费者投递，它离开后由另一个消费者接替。
描述符没有对应的方法，所以 [`argument`](../components/subscriptions/arguments.md) 把 `x-single-active-consumer` 原样传给队列声明。
其他实例保持订阅并处于空闲状态，直到轮到它们接替。

这个参数是队列声明的一部分。
在 `declare_topology(true)` 下，服务带着它声明队列；由运维创建的队列则从运维的声明中获得它（[拓扑](topology.md)）。

## 测试 {/* #testing */}

示例文件中的测试用 `with_headers` 发布带键的发票，并检查同一个客户的发票按顺序到达处理器。
进程内 RabbitMQ 像服务器一样在竞争消费者之间共享队列，并组装批。
预取窗口和单活跃消费者由服务器实现，所以要用[真实服务器上的测试](../components/testing/live.md)来检查。
[测试 RabbitMQ 服务](testing.md)讲解了测试套件。
