---
title: 消费者与预取
sidebar_position: 2
description: "队列向消费者推送投递，每个消费者都有一个未确认投递的窗口，确认会重新打开窗口。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "队列向两个预取窗口均为 2 的消费者推送投递；一次 ack 腾出一个槽位，关闭的信道交还它的投递",
  "text": "以文字阅读本图",
  "queue": ["队列 orders", "消息按顺序等待"],
  "consumer1": ["消费者 1", "预取 2：", "2 条未确认"],
  "consumer2": ["消费者 2", "预取 2：", "2 条未确认"],
  "ack": ["basic.ack 腾出", "一个槽位，下一条", "投递随即推送"],
  "closed": ["信道关闭：", "未确认的投递", "回到队列，", "标记为重新投递"],
  "roundRobin": ["队列轮流把投递分给各个消费者，", "跳过窗口已满的消费者"],
  "deliver": "basic.deliver"
};

# 消费者与预取

队列向消费者推送投递，每个消费者都有一个未确认投递的窗口，确认会重新打开窗口。

<Diagram name="consumers" words={words} />

消费者不主动索取消息。它用 `basic.consume` 注册，队列向它推送 `basic.deliver` 帧。同一个队列上的多个消费者相互竞争：队列把每条消息轮流交给其中一个。

预取窗口是一个消费者可以持有的未确认投递数。窗口满了之后，队列不再向这个消费者推送任何东西，直到一次确认腾出一个槽位。慢的处理器正是这样让队列放慢，而不是塞满服务的内存。RabbitMQ 用 `basic.qos` 为每个消费者设置窗口，0 表示不限制。

投递在结算之前一直属于这个消费者。`basic.ack` 把消息从队列中移除。带 `requeue` 的 `basic.reject` 把它放回队列，不带 `requeue` 时，队列丢弃它或把它转为死信。信道关闭时如果还有未结算的投递，队列会把它们收回，并在设置 `redelivered` 标志后再次投递。

## 在本 crate 中 {/* #in-this-crate */}

每个订阅打开自己的信道，并在上面启动一个消费者。在消费者启动之前，订阅用 `basic.qos` 发送它的窗口：[`RabbitQueue::prefetch`](api:ruststream_lapin::RabbitQueue::prefetch) 用于单个队列，[`LapinBroker::prefetch`](api:ruststream_lapin::LapinBroker::prefetch) 用于所有没有自行设置的订阅。这个设置是一个非零数，所以两者都没有设置的订阅不发送 `basic.qos`，以不限制的方式运行。

窗口也限制批的大小。AMQP 没有批帧，所以批处理器的一批在客户端上，从队列已经推送过来的投递中组装：最多到挂载点指定的大小，或者在 [`batch_wait`](api:ruststream_lapin::RabbitQueue::batch_wait)（默认 50 毫秒）内到达的数量。窗口小于批大小时，批的上限就是窗口。

订阅被释放时关闭它的信道，RabbitMQ 把它持有而未结算的投递重新入队。运行时如何结算每次投递，见[结算与重试](../settlement.md)。

协议规则来自 RabbitMQ（[消费者预取](https://www.rabbitmq.com/docs/consumer-prefetch)、[消费者确认](https://www.rabbitmq.com/docs/confirms#consumer-acknowledgements)、[工作队列](https://www.rabbitmq.com/tutorials/tutorial-two-python)）。
