---
title: 测试 RabbitMQ 服务
sidebar_position: 8
description: "用 TestApp 在进程内测试 RabbitMQ 服务：生产环境的应用、结果、回复、延迟，以及在运行中的 RabbitMQ 上执行的同一个测试。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 测试 RabbitMQ 服务

测试用 [`TestApp`](api:ruststream::testing::TestApp) 运行服务自己的应用，连接的是[进程内 RabbitMQ](../components/testing/index.md) 或者运行中的服务器。

被测服务负责银行卡扣款。
付款到达 `card-payments` 队列，处理器在 `card-receipts` 上以一张收据作为回复。
没有金额的付款被丢弃，队列把它移到 `card-payments.review`。
超出银行即时限额的付款会被暂缓：处理器用 [`retry_after`](../components/settlement/retry-after.md) 要求一分钟后再投递它。
这一分钟在队列的等待队列 `card-payments.retry` 中度过。
应用由 `service(broker)` 构建，所以测试可以选择 Broker，而 `main` 使用生产环境的 Broker。
测试需要在 `[dev-dependencies]` 中启用 `ruststream-lapin` 的 `testing` feature。

## 一笔付款及其收据 {/* #a-payment-and-its-receipt */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/testing.rs#in_process"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#in_process"
```

</TabItem>
</Tabs>

[`TestApp::start`](api:ruststream::testing::TestApp::start) 把 [`LapinBroker`](../components/broker/index.md) 连接到进程内 RabbitMQ。
URI 会像建立连接时那样被解析，但从不拨号。
测试按生产类型访问 Broker：`tb.broker::<LapinBroker>()`。
`Payment` 声明了自己的目的地 `card-payments`，所以测试发布它时不需要 `to(..)`。
`publish` 在处理器及其回复都完成结算后才返回。
所以接下来的几行断言的是已经完成的状态：订阅者如何结算，以及什么到达了 `card-receipts`。
测试主体把已启动的应用作为参数接收，所以下面的真实服务器测试也运行它。

## 被丢弃的付款 {/* #a-dropped-payment */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/testing.rs#dropped"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#dropped"
```

</TabItem>
</Tabs>

`settled` 检查处理器返回的结果。
`assert_not_called` 检查没有发布任何收据。

## 不必等待的延迟 {/* #a-delay-without-waiting-for-it */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/testing.rs#held"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#held"
```

</TabItem>
</Tabs>

测试从暂停的时钟开始，[`advance`](api:ruststream::testing::TestApp::advance) 把时钟向前拨。
一分钟的 `HOLD` 不花任何时间。
队列设置了 [`delay`](../components/settlement/delay.md)，所以进程内 RabbitMQ 把副本留在等待队列中，延迟结束时把它释放到 `card-payments`。
副本属于 Broker，所以 `published` 只显示测试自己发布的消息。
每次被暂缓的投递都记录为 `Outcome::Nack`。

## 进程内 RabbitMQ 模拟了什么 {/* #what-the-in-process-rabbitmq-models */}

进程内 RabbitMQ 读取 Broker 的设置和各路由的描述符，与服务器看到的是同一份。
[`declare_topology`](../components/broker/declare-topology.md) 决定订阅是否声明自己的队列，与在服务器上一样。
它模拟的是 RabbitMQ，而不是一个通用队列：

- 默认、direct、topic、fanout 和 headers 交换机按描述符声明的绑定进行路由。
- 竞争消费者共享一个队列，重新入队的消息再次经过这个队列。
- 没有人消费时，队列保留到达它的消息；关闭的消费者把它取走而未结算的消息交还。
- 被丢弃的消息经由其队列的死信路线离开；仲裁队列统计投递次数，并以同样的方式把用尽次数的消息转为死信。
- 等待队列释放延迟副本，direct reply-to 为每个请求分配它自己的地址。
- 事务在提交之前不可见。

服务器拒绝的，它也拒绝：协议无法承载的名字或消息头、超过 16 MiB 的消息体、非排他的非持久队列、默认交换机上的绑定、以不同设置再次声明的队列、带优先级的仲裁队列。
在连接关闭之后结算的投递会报告信道已关闭。

只有服务器才具备的东西需要服务器来测试：预取窗口、发布者确认、信道事务的原子性、插件交换机、两个连接共享的队列，以及服务不声明就期望找到的队列。

## 在服务器上运行同一个测试 {/* #the-same-test-on-a-server */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/testing.rs#live"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#live"
```

</TabItem>
</Tabs>

[`TestApp::start_live`](../components/testing/live.md) 把同一个应用连接到运行中的 RabbitMQ。
测试用 `live::url`（示例自带的辅助函数）从 `AMQP_TEST_URL` 读取 URI，未设置时跳过。
Broker 声明自己的拓扑，所以测试可以在空的服务器上运行。
断言与进程内测试的完全相同。
在服务器上，由 RabbitMQ 决定队列的哪个消费者取走消息，所以真实服务器测试每个队列只运行一个订阅。

```bash
cargo test
AMQP_TEST_URL=amqp://127.0.0.1:5672 cargo test -- --test-threads=1
```

[快速开始](getting-started.md)中的 RabbitMQ 就足以完成真实服务器上的运行。
