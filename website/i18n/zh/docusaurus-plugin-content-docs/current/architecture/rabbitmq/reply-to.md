---
title: Direct reply-to
sidebar_position: 5
description: "direct reply-to 把回复直接发到请求方的信道，不经过回复队列，至多一次。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "经由 direct reply-to 的一次请求：请求经过普通队列，回复直接回到请求方的信道",
  "text": "以文字阅读本图",
  "consume": ["请求方信道消费", "amq.rabbitmq.reply-to，不确认"],
  "request": ["请求在同一信道上发布，", "带 reply-to 和 correlation-id"],
  "requests": ["请求队列", "一个普通队列"],
  "responder": ["应答方处理器", "回复上挂 DirectReplyTo"],
  "reply": ["回复发往默认交换机，", "路由键 = 收到的 reply-to"],
  "back": ["请求方信道", "直接收到，中间没有队列"],
  "resolved": ["请求完成", "按 correlation-id 匹配"],
  "atMostOnce": ["回复只存在于请求方的信道中：", "信道消失后发出的回复会丢失"]
};

# Direct reply-to

direct reply-to 把回复直接发到请求方的信道，不经过回复队列，至多一次。

<Diagram name="replyTo" words={words} />

请求方以 no-ack 模式消费伪队列 `amq.rabbitmq.reply-to`。这个伪队列无需声明。在同一个信道上发布、`reply-to` 设为这个名字的请求，会得到一个私有地址：RabbitMQ 把这个属性改写为 `amq.rabbitmq.reply-to.` 加上它自己生成的后缀。

请求像任何消息一样传递：经过交换机进入队列，再到达消费者。应答方把回复发布到默认交换机，以收到的 `reply-to` 作为路由键。RabbitMQ 把回复直接交给请求方的信道。中间没有任何队列保存它。

回复至多投递一次。如果请求方的信道已经不在，回复会被丢弃，请求方只能重新发送请求。

## 在本 crate 中 {/* #in-this-crate */}

[`LapinRequester`](api:ruststream_lapin::LapinRequester) 在第一个请求时打开自己的信道，在上面启动回复消费者，并在这个信道上发布每个请求。每个请求带有一个生成的 `correlation-id`，id 相同的回复完成这次调用。每个请求方用一个任务读取回复，这个任务运行在 Broker 建立连接时所在的运行时上。信道错误会关闭信道，丢失仍在途中的回复；下一个请求会打开新信道。回复始终不来的请求，由每个请求的超时来结束。

请求默认是非持久化的：超时之后没人再等的请求，在重启后存活下来也没有意义。[`LapinRequest::persistent`](api:ruststream_lapin::LapinRequest::persistent) 开启持久化。

应答方是一个带回复的普通处理器。[`DirectReplyTo`](api:ruststream_lapin::DirectReplyTo) 作为回复的转换挂载，把每条回复发送到请求指定的地址，并带回请求的 `correlation-id`。回复到达的是一个不需要确认的消费者，所以结算它不做任何事。

协议规则来自 RabbitMQ（[direct reply-to](https://www.rabbitmq.com/docs/direct-reply-to)）。
