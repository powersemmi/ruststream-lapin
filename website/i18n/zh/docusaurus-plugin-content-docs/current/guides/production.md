---
title: 在生产环境中运行
sidebar_position: 9
description: "在生产环境中运行 RabbitMQ 服务：连接 URI、TLS、命名连接、预取、信道、连接丢失和停机。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 在生产环境中运行

本指南为生产环境配置 RabbitMQ 服务的连接，并说明连接或服务消失时它会怎么做。

## 连接 {/* #the-connection */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/production.rs#broker"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/production.rs#broker"
```

</TabItem>
</Tabs>

[连接 URI](../components/broker/connection.md) 包含连接的全部信息：协议、凭据、主机、端口和虚拟主机。
`amqp://orders:secret@rabbit.internal:5672/shop` 以 `orders` 身份登录虚拟主机 `shop`。
默认虚拟主机 `/` 写作 `%2f`。
RabbitMQ 的 [URI 规范](https://www.rabbitmq.com/docs/uri-spec)列出了查询参数，例如 `heartbeat`。
服务从 `AMQP_URL` 读取 URI，所以密钥不会出现在代码里。

[`connection_name`](api:ruststream_lapin::LapinBroker::connection_name) 在管理界面中为连接加上标签，运维人员可以看到是哪个服务、哪个版本持有它。
[`prefetch`](../components/subscriptions/prefetch.md) 限制每个订阅的未确认投递数，慢的服务正是靠它向服务器施加背压（[工作队列](work-queues.md)）。

`LapinBroker::new` 只记录 URI。
服务在启动时建立连接；URI 无法解析或服务器无法访问时，启动以 [`AmqpError::Connect`](../components/broker/errors.md) 错误中止。

## TLS {/* #tls */}

`amqps://` URI 通过 TLS 连接，端口通常是 5671。
TLS 由 `ruststream-lapin` 的三个 feature 之一提供，每个都对应 lapin 自己的 feature：

| Feature | TLS 库 |
|---|---|
| `tls-native-tls` | 平台自带的库：Linux 上是 OpenSSL，macOS 上是 Secure Transport，Windows 上是 SChannel |
| `tls-rustls` | rustls |
| `tls-rustls-ring` | 使用 `ring` 加密后端的 rustls |

服务器证书按所选库的受信任根证书进行校验。
RabbitMQ 的 [TLS 指南](https://www.rabbitmq.com/docs/ssl)讲解了服务器端的配置。
[TLS 组件](../components/broker/tls.md)给出了依赖声明的写法。

服务的 AsyncAPI 文档报告主机和 AMQP 版本，从不报告 URI 中的凭据或虚拟主机：这份文档会被公开和分享。

## 生产环境中的拓扑 {/* #topology-in-production */}

生产服务通常关闭 `declare_topology` 运行，使用由运维创建和修改的队列。
这时不存在的队列会让启动失败，而不是丢失消息（[拓扑](topology.md)）。
在这种部署方式下，仲裁队列的尝试次数上限和延迟重试的等待队列同样由服务器一侧负责（[确认、重试与死信](retries.md)）。

## 连接与信道 {/* #connections-and-channels */}

一个连接承载整个服务。
每个订阅打开自己的信道，所以一个队列上的错误不会让其他队列停下来。
发后即忘的发布共用一个信道。
发布者确认模式的发布者、信道事务的发布者和请求方各自在首次使用时打开自己的信道，因为发布者确认、事务和 direct reply-to 都是信道的状态。

遇到信道级错误时，例如发布到不存在的交换机，RabbitMQ 关闭这个信道。
连接保持不变，发布者在下一次调用时打开新的信道。

## 连接丢失 {/* #connection-loss */}

连接恢复默认关闭。启用 `LapinBroker::auto_recover(true)` 后，Lapin 使用指数退避重新连接，首次连接也会重试。
默认上限为 16 次重试；`.recovery_retries(n)` 可以调整上限，零表示不重试。
Lapin 通过现有句柄恢复信道、已声明的交换机、队列、绑定、预取、发布者确认和消费者。它不会重启应用，也不会重新运行启动钩子。

在 URI 中配置心跳，例如 `amqp://localhost:5672?heartbeat=30`，以检测静默断连。
`ConnectedLapinBroker::is_connected()` 可供就绪检查使用：断连期间或停机后返回 false。
返回 true 并不表示信道和拓扑已恢复完毕，也不表示工作任务正常；这项检查与运行时的服务健康状态不同。

失败的发布不会自动重发，已发出的 RPC 请求的截止时间继续计算。
`ServerTxPublish` 要求 `auto_recover(false)`，因为事务状态在重连后会丢失。
启动时耗尽重试次数会导致连接失败；启动后耗尽次数则使消费者流返回终止的消费错误并结束。

仍然存在的队列中的未确认投递会在断连后返回 RabbitMQ，并可能带着 [`Redelivered`](../components/context/index.md) 标记再次投递。
重新投递要求处理器容忍重复。服务器在正常信道上取消消费者（例如删除其队列）时，订阅结束。

## 停机 {/* #shutdown */}

`SIGTERM` 或 <kbd>Ctrl</kbd>+<kbd>C</kbd> 停止服务。
运行时停止各个订阅，并以完整的握手关闭连接。
RabbitMQ 把服务取走而未结算的每次投递交还给它的队列，由另一个实例取走。
比连接活得更久的发布者或请求方，例如后台任务持有的那个，会返回 [`AmqpError::Closed`](../components/broker/shutdown.md)，而不是向已关闭的连接发送。

如果在重连期间开始停机，等待中的消费者会被唤醒，broker 停机无需等待 RabbitMQ 恢复。现有句柄立即视为已关闭；之后恢复的连接会在后台关闭，重试次数耗尽时清理也会结束。

## 测试 {/* #testing */}

示例文件中的测试分别用带凭据的 `amqps://` URI 和格式错误的 URI 启动服务。
在进程内，URI 的解析方式与建立连接时相同，所以服务无法用来连接的 URI 也无法启动测试。
TLS 和连接丢失需要服务器，所以要用[真实服务器上的测试](../components/testing/live.md)来检查。
