---
title: 确认、重试与死信
sidebar_position: 5
description: "用原生帧结算 RabbitMQ 投递，立即或延迟重试，限定尝试次数，并把一直失败的消息转入死信队列。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 确认、重试与死信

本指南结算 RabbitMQ 投递，立即或稍后重试，限定尝试次数，并把一直失败的消息转入[死信队列](../components/settlement/retries.md)。

示例是一个退款服务。
退款到达一个经典队列，打款到达一个仲裁队列，两者在五次投递之后都进入死信队列。

## 结果发送什么 {/* #what-an-outcome-sends */}

处理器的[结果](../components/settlement/index.md)变成投递所在信道上的一个 AMQP 帧：

| 结果 | 帧 | RabbitMQ 做什么 |
|---|---|---|
| `HandlerOutcome::ack()` | `basic.ack` | 把消息从队列中移除 |
| `HandlerOutcome::retry()` | 带 requeue 的 `basic.reject` | 把消息放回队列 |
| `HandlerOutcome::drop()` | 不带 requeue 的 `basic.reject` | 移除消息；如果队列有[死信交换机](../components/settlement/dead-letter-exchange.md)，就把它转为死信 |
| `HandlerOutcome::retry_after(delay)` | 一次已确认的投递加一份延迟副本 | 延迟过去后再次投递副本 |

RabbitMQ 把每次拒绝都算作这条消息用掉的一次投递。
所以一个不断要求退回消息的处理器，会耗尽仲裁队列自己的上限。
服务取走而从未结算的投递，会在信道或连接关闭时回到队列。

## 订阅者 {/* #the-subscriber */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/retries.rs#refund"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#refund"
```

</TabItem>
</Tabs>

发卡处理商有三种拒绝退款的方式，每种需要不同的结果。
`Busy` 表示它的速率限额已经用完：立即重试仍会被拒绝，所以 [`retry_after`](../components/settlement/retry-after.md) 等待 30 秒。
`Unavailable` 表示它没有应答：`retry` 立即再试一次。
`Declined` 表示卡账户已经关闭，再试一次也是同样的答复，所以这笔退款被丢弃。

## 经典队列上的上限 {/* #the-cap-on-a-classic-queue */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/retries.rs#app"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#app"
```

</TabItem>
</Tabs>

[`max_attempts`](api:ruststream::runtime::RouterWith::max_attempts) 统计一笔退款的每次投递，第一次也算在内。
第五次投递不再重试：[`dead_letter`](api:ruststream::runtime::RouterWith::dead_letter) 把它发布到 `refunds.dead`。
目的地是默认交换机上的路由键，所以这笔退款落入同名的队列。

经典队列什么都不统计，所以[上限](../components/settlement/retries.md)由服务负责。
在有上限时，重试会确认这次投递，并以队列自己的名字把一份副本发布回该队列。
副本在 `x-ruststream-retry-count` 消息头（[`RETRY_COUNT_HEADER`](api:ruststream::runtime::RETRY_COUNT_HEADER)）中带着尝试次数，其值加一。
副本通过 Broker 的默认发布策略发出；`.out_retry(policy)` 可以指定另一个策略，例如用 [`TransactionalPublish`](../components/transactions/confirms-publish.md) 等待 RabbitMQ 确认每份副本。

被丢弃的退款不会到达 `refunds.dead`：死信队列只存放用完了尝试次数的退款。
如果还想保留被丢弃的消息，就给队列配置它自己的死信交换机（[拓扑](topology.md)）。

## 仲裁队列上的上限 {/* #the-cap-on-a-quorum-queue */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/retries.rs#payout"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#payout"
```

</TabItem>
</Tabs>

仲裁队列自己统计每条消息的投递次数。
在 `payouts` 上，挂载点上同样的两个步骤变成队列的 `x-delivery-limit` 及其死信路线。
于是服务器在一笔打款第五次投递之后把它转入 `payouts.dead`，无论服务是否在运行都是如此，服务本身不发布任何东西。
重试仍然是一个普通的带 requeue 的 `basic.reject`。

这些参数只有在服务声明队列时才会写到队列上，所以这需要 [`declare_topology(true)`](../components/broker/declare-topology.md)。
在不做任何声明的 Broker 上，服务拒绝启动：在别处声明的队列带着其所有者的参数，上限将无处生效。
这种情况下，在队列本身上设置上限和路线，通过它的声明或一条策略（policy），并且挂载处理器时不加这两个步骤。
只声明一半也会被拒绝，因为有上限却无处发送用尽的消息，就会把它丢弃。
`.out_retry(..)` 在仲裁队列上无法通过编译：这里没有要发布的副本。

没有声明上限的仲裁队列采用 RabbitMQ 4 的默认值，即 20 次投递；[`delivery_limit`](../components/settlement/delivery-limit.md) 在描述符上设置另一个值。
仲裁队列的投递通过 `redelivery_count()` 报告它已经用掉了多少次投递。
经典队列的投递只报告 [`Redelivered`](../components/context/index.md)，即 RabbitMQ 之前是否投递过它。

## 延迟重试 {/* #delayed-retries */}

AMQP 没有单条消息的延迟。
队列上没有 `delay` 时，`retry_after` 把副本留在服务里，延迟过去后再发布它。
这份副本是至多一次的：在延迟期间停止的服务会丢失它。

[`delay(Delay::dlx_ttl())`](../components/settlement/delay.md) 把等待移到 RabbitMQ 上。
副本在 `refunds.retry` 队列中按单条消息的 TTL 等待，TTL 到期时，等待队列把它作为死信送回 `refunds`。
原始消息在副本于同一信道上发布之后才被确认，所以两者之间出现故障时，这笔退款会重复，而不会丢失。
副本是一条新消息，所以它带着加一后的重试次数，上限像结束立即重试的循环一样结束延迟重试的循环。

等待队列只从队头释放消息。
延迟长的消息会挡住排在它后面的每条消息，所以延迟长短不一时，每种延迟各用一个等待队列，用 [`Delay::dlx_ttl_named`](api:ruststream_lapin::Delay::dlx_ttl_named) 命名。
`plugin-dme` feature 加入 [`Delay::plugin_dme`](../components/settlement/plugin-dme.md)，它在 RabbitMQ 的延迟消息交换机插件中等待，在那里短延迟从不排在长延迟后面。

等待队列和延迟交换机都属于基础设施。
在 `declare_topology(true)` 下，服务声明它们。
关闭声明时，带 `delay` 的订阅在启动时检查它的等待队列或延迟交换机是否存在，缺少时服务不会启动。

## 读取死信队列 {/* #reading-the-dead-letter-queue */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/retries.rs#dead_letters"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#dead_letters"
```

</TabItem>
</Tabs>

死信队列是一个普通队列，处理器像读取其他队列一样读取它。
由服务转入死信的退款带着重试次数消息头，其值是它经历的投递次数，这里是 5。
由仲裁队列转入死信的打款则带着 RabbitMQ 自己的 `x-death` 消息头。
两个处理器都把消息记入日志，留给人来查看。

## 测试 {/* #testing */}

示例文件中的测试在暂停的时钟上于进程内运行应用，所以 `tb.advance(BACKOFF)` 让 30 秒瞬间过去。
进程内 RabbitMQ 像服务器一样释放等待队列中的副本，统计仲裁队列的投递次数，并把用尽次数的消息经由其路线转为死信。
延迟消息交换机插件只在服务器上工作，所以它的测试针对真实的 RabbitMQ 运行。
[测试 RabbitMQ 服务](testing.md)讲解了测试套件。
