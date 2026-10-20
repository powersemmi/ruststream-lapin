---
title: 生命周期
sidebar_position: 3
description: "LapinBroker::new 记录配置，connect 打开连接，启动时为每个订阅打开一个信道和一个消费者，停机时关闭。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "Broker 的三个状态作为类型，以及每次转换在 RabbitMQ 上做什么",
  "text": "以文字阅读本图",
  "states": "Broker",
  "rabbit": "在 RabbitMQ 上",
  "unconnected": ["LapinBroker::new", "(uri)"],
  "connected": ["ConnectedLapinBroker"],
  "closed": ["ClosedLapinBroker", "handshake()"],
  "config": ["只记录配置，", "无 I/O"],
  "connection": ["连接和", "发布信道"],
  "subscriptions": ["每个订阅：", "自己的信道，", "开启声明时的拓扑，", "basic.qos 和", "basic.consume"],
  "ownChannels": ["第一次带确认的发布、", "事务或请求", "打开它的信道"],
  "close": ["先标记为关闭，", "再 connection.close"],
  "delayCheck": ["不做声明时，缺少等待队列", "或延迟交换机会让启动失败"],
  "afterShutdown": ["比连接活得更久的发布者", "返回 AmqpError::Closed，绝不默默成功"],
  "connect": "connect(self)",
  "shutdown": "shutdown(self)"
};

# 生命周期

[`LapinBroker::new`](api:ruststream_lapin::LapinBroker::new) 记录配置，connect 打开连接，启动时为每个订阅打开一个信道和一个消费者，停机时关闭。

<Diagram name="lifecycle" words={words} />

`new` 不做 I/O，所以服务的构建器保持同步。运行时在应用启动时连接 Broker。connect 打开到该 URI 的 AMQP 连接，带上管理界面所显示的 [`connection_name`](api:ruststream_lapin::LapinBroker::connection_name)，并打开共用的发布信道。服务器拒绝连接时，启动失败。

每个订阅的描述符针对已连接的 Broker 解析一次，遇到第一个无法打开的订阅，启动就失败：

1. 仲裁队列检查它的重试声明。`max_attempts` 和 `dead_letter` 会变成队列自己的参数，所以两者必须同时出现，并且需要一个会声明该队列的 Broker。
2. 订阅打开它的信道。
3. 在 [`declare_topology(true)`](api:ruststream_lapin::LapinBroker::declare_topology) 下，它声明自己的交换机、队列、绑定和延迟目标。与服务器上已有内容不一致的声明会被 `PRECONDITION_FAILED` 拒绝。
4. 如果设置了预取窗口，它发送带窗口值的 `basic.qos`。
5. 对于 Broker 不负责声明的 [`delay`](api:ruststream_lapin::RabbitQueue::delay)，检查它的等待队列或延迟交换机是否存在。缺少时启动失败，错误同时指出订阅和目标。
6. 它用 `basic.consume` 启动消费者。不存在的队列会在这一步失败。

之所以要检查延迟目标，是因为延迟副本发布时不等待应答，而原始消息在副本之后才被确认。服务器会丢弃发往不存在的等待队列的副本，所以没有这项检查，消息会在没有任何错误的情况下丢失。这项检查在启动时花一次往返，在每条消息上不花任何开销。

只消费的服务不再打开其他信道。第一次经由[发布者确认或事务策略](rabbitmq/publishing.md)的发布，以及第一个请求，各自打开所需的信道。

停机时先把连接标记为关闭，这样仍持有它的发布者无法再塞进一条消息，然后用 AMQP 的关闭握手关闭连接。服务器把各个订阅持有而未结算的消息重新入队。[`ClosedLapinBroker::handshake`](api:ruststream_lapin::ClosedLapinBroker::handshake) 报告握手是否完成。停机后，比连接活得更久的发布者或请求方返回 [`AmqpError::Closed`](api:ruststream_lapin::AmqpError::Closed)，停机后才结算的投递返回错误，而不是报告一次从未发生的结算。

## 连接恢复 {/* #connection-recovery */}

恢复默认关闭。启用 `LapinBroker::auto_recover(true)` 后，已连接的 broker 可以处于已连接或正在重连的状态，而 Rust 类型不变。
`is_connected()` 报告 AMQP 连接状态；即使返回 true，信道和拓扑恢复也可能仍在进行。
Lapin 恢复现有信道及其拓扑，不会重新构建应用，也不会重新运行启动钩子。
`recovery_retries(n)` 限制启动及后续恢复的重试次数（默认为 16）。次数耗尽时，消费者流以终止的消费错误结束。
恢复期间停机会立即关闭现有句柄并唤醒等待的消费者；稍后重新连接的套接字会在后台关闭。
