---
title: 路径
sidebar_position: 4
description: "一次 RabbitMQ 投递在 RustStream 服务中的路径：推送、处理器、回复和确认。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "一次需要回复的 RabbitMQ 投递的路径：从队列到它的确认",
  "text": "以文字阅读本图",
  "queue": ["队列", "在预取窗口内推送"],
  "deliver": ["basic.deliver", "在订阅自己的信道上"],
  "message": ["LapinMessage", "保留消息体，到达时读取消息头"],
  "dispatch": ["分发", "一个循环，或每个键一个工作分区"],
  "decode": ["解码", "编解码器读取消息体"],
  "body": ["订阅者的处理器", "#[subscriber] 下的 async fn"],
  "encode": ["回复", "编码进循环的缓冲区"],
  "publisher": ["LapinPublisher", "以 basic.publish 发出回复"],
  "settle": ["结算", "basic.ack 或 basic.reject"]
};

# 路径

一次 RabbitMQ 投递在 RustStream 服务中的路径：推送、处理器、回复和确认。

<Diagram name="path" words={words} />

只要订阅的[预取窗口](rabbitmq/consumers.md)还有余量，队列就把投递推送给订阅的消费者。lapin 交出投递时，消息体放在它自己的缓冲区里。[`LapinMessage`](api:ruststream_lapin::LapinMessage) 把这个缓冲区原样作为消息体保留，不做复制。它在投递到达时读取消息头：消息头表中的字符串和字节条目，以及处理器以消息头形式读取的原生属性（`content-type`、`correlation-id`、`reply-to`、`message-id`、优先级和过期时间）。交换机、路由键、`redelivered` 标志和投递标签作为投递的上下文与它们放在一起。

运行时从订阅的流中取出投递，在它的[分发循环](lanes.md)中运行。编解码器解码消息体，然后处理器运行。

回复编码进分发循环复用的缓冲区。[发布者](rabbitmq/publishing.md)把这些字节借给 lapin，lapin 把它们复制进要发送的帧。四个标识类消息头放在各自的原生属性中发出，所以不是 RustStream 服务的消费者也能在 AMQP 规定的位置找到它们。其余每个消息头都以字节串的形式写入消息头表。路由键是回复的目的地，交换机则来自发布策略：除非策略指定了另一个，否则就是默认交换机。

投递在回复之后、在它到达的那个信道上结算。`ack` 发送 `basic.ack`。`retry()` 和 `drop` 发送 `basic.reject`，分别带和不带 `requeue`。回复发布失败时，这次投递以重试的方式结算。[结算与重试](settlement.md)用图说明了每种结果会变成什么。

与裸 lapin 客户端相比，这条路径在每条消息上的开销见[基准测试](benchmarks/index.md)。
