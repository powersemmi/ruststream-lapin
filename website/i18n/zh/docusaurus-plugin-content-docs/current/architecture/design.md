---
title: 设计
sidebar_position: 2
description: "ruststream-lapin 所处的位置：位于 RustStream 运行时和 lapin 客户端之间，一个连接，每个订阅一个信道。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "自上而下依次是服务、RustStream 运行时、本 crate、lapin 客户端和 RabbitMQ 节点",
  "text": "以文字阅读本图",
  "service": "你的服务",
  "handlers": ["#[subscriber]", "订阅者处理器"],
  "app": ["RustStream::new(..)", ".with_broker(..)"],
  "core": "RustStream 核心",
  "runtime": "运行时：分发、工作分区、结算",
  "crate": "ruststream-lapin",
  "subscriber": ["LapinSubscriber", "产出 LapinMessage"],
  "publishers": ["LapinPublisher,", "ConfirmsPublisher,", "ServerTxPublisher"],
  "broker": ["LapinBroker", "配置"],
  "descriptors": ["RabbitQueue,", "RabbitQuorumQueue,", "RabbitExchange"],
  "requester": ["LapinRequester"],
  "client": "lapin",
  "connection": ["连接", "每个 Broker 一个"],
  "subChannels": ["每个订阅", "一个信道"],
  "pubChannels": ["共用的发布信道；", "发布者确认、事务", "和请求各有自己的"],
  "rabbitmq": "RabbitMQ 节点",
  "mounted": "挂载到",
  "polls": "轮询流"
};

# 设计

`ruststream-lapin` 所处的位置：位于 RustStream 运行时和 lapin 客户端之间，一个连接，每个订阅一个信道。

<Diagram name="design" words={words} />

本 crate 在 [lapin](https://docs.rs/lapin/4.12.0/lapin/) 之上实现核心的 Broker trait，lapin 是一个纯 Rust 的 AMQP 0.9.1 客户端。它上面的运行时对每个 Broker 都相同。所有 RabbitMQ 特有的内容都在本 crate 中：[队列描述符](rabbitmq/queues.md)、结算所发送的帧、重试的去向，以及三种[发布模式](rabbitmq/publishing.md)。

一个 Broker 打开一个连接。每个订阅在自己的信道上消费，所以它的预取窗口和它的错误都只属于它自己。发后即忘的发布者共用一个信道。发布者确认模式的发布者、事务发布者和请求方各自在首次使用时打开自己的信道，因为确认模式、事务和回复消费者都是信道状态。

信道错误只关闭一个信道，连接保持不变。发布者的信道在下一次使用时重新打开，所以一次发往不存在的交换机的发布不会让服务停止发布。

[`LapinBroker`](api:ruststream_lapin::LapinBroker) 默认什么都不声明。这时描述符说明服务期望的拓扑，缺少的队列会让它的订阅失败。[`declare_topology(true)`](api:ruststream_lapin::LapinBroker::declare_topology) 让每个订阅依次声明它的交换机、队列和绑定。

请求方在 Broker 建立连接时所在的运行时上，用一个任务读取回复，与哪个线程发出第一个请求无关。

各个部件如何启动和停止，见[生命周期](lifecycle.md)；一次投递的路径，见[路径](path.md)。
