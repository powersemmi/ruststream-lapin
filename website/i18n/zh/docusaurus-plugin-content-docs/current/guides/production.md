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

与 RabbitMQ 的连接丢失时，无论是服务器重启、故障转移，还是运维人员关闭了它，服务都会自行重新连接。
每个订阅重新连接服务器，以相同的声明和预取设置重新打开它的消费者。
它以逐渐增长的间隔重试：从 100 ms 开始翻倍，最长 10 s；每次失败记录一条警告，恢复后记录一条 info 日志。
发布者和请求方在下一次调用时重新连接，服务器无法访问期间返回错误。
中断期间，服务的健康状态保持为 `Running`。

服务已取走而未结算的投递，会在连接断开时回到队列，RabbitMQ 再次投递它们，并标记为 [`Redelivered`](../components/context/index.md)。
所以在连接断开时正在处理某条消息的处理器，可能会再次收到它。
如果服务器在仍然存活的信道上取消了某个消费者（因为它的队列被删除），这个消费者的订阅随之结束。

## 停机 {/* #shutdown */}

`SIGTERM` 或 <kbd>Ctrl</kbd>+<kbd>C</kbd> 停止服务。
运行时停止各个订阅，并以完整的握手关闭连接。
RabbitMQ 把服务取走而未结算的每次投递交还给它的队列，由另一个实例取走。
比连接活得更久的发布者或请求方，例如后台任务持有的那个，会返回 [`AmqpError::Closed`](../components/broker/shutdown.md)，而不是向已关闭的连接发送。

## 测试 {/* #testing */}

示例文件中的测试分别用带凭据的 `amqps://` URI 和格式错误的 URI 启动服务。
在进程内，URI 的解析方式与建立连接时相同，所以服务无法用来连接的 URI 也无法启动测试。
TLS 和连接丢失需要服务器，所以要用[真实服务器上的测试](../components/testing/live.md)来检查。
