---
title: 经典队列与仲裁队列
sidebar_position: 3
description: "经典队列位于一个节点上，什么都不统计；仲裁队列是复制的，会统计失败的投递，并自行移走用尽次数的消息。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "并排对比经典队列与仲裁队列：各自在哪里保存消息，重新入队做了什么，以及什么会结束一条反复回来的消息",
  "text": "以文字阅读本图",
  "classic": "经典队列",
  "quorum": "仲裁队列",
  "classicStore": ["一个节点", "保存消息"],
  "classicRequeue": ["带 requeue 的 reject：", "回到队列，", "不计数"],
  "classicReject": ["不带 requeue 的", "reject：转为", "死信或丢弃"],
  "quorumStore": ["一个主节点及其", "从节点，基于 Raft"],
  "quorumRequeue": ["带 requeue 的 reject：", "回到队列，", "x-delivery-count + 1"],
  "quorumSpent": ["超过投递", "上限：转为", "死信或丢弃"],
  "limit": ["从 RabbitMQ 4.0 起，仲裁队列的投递上限", "默认为 20，除非队列另行设置"],
  "spent": "超过上限"
};

# 经典队列与仲裁队列

经典队列位于一个节点上，什么都不统计；仲裁队列是复制的，会统计失败的投递，并自行移走用尽次数的消息。

<Diagram name="queues" words={words} />

队列的类型在声明时通过 `x-queue-type` 参数确定。两种类型接受同样的消费者和同样的结算帧。它们的区别在于消息保存在哪里，以及重新入队让消息付出什么代价。

在 RabbitMQ 4 中，经典队列把消息保存在一个节点上，不做复制。带 `requeue` 被拒绝的投递会回到队列，并且可以无休止地反复回来。队列不保存计数：重新投递只带有 `redelivered` 标志。

仲裁队列用 Raft 协议把消息复制到多个节点，每次写入都要得到其中法定数量节点的接受。它天然是持久的。它统计每条消息失败的投递次数，并在重新投递时把计数写进 `x-delivery-count` 消息头。计数超过队列投递上限的消息会离开队列。从 RabbitMQ 4.0 起，队列没有设置上限时，上限为 20。

从 RabbitMQ 4.3 起，并不是每次退回都计数。`basic.reject`，以及在投递未结算时关闭的信道，都会计数。带 `requeue` 的 `basic.nack` 退回消息但不计数。

因不带 `requeue` 的拒绝或超过投递上限而离开队列的消息，在队列有死信交换机时进入该交换机（`x-dead-letter-exchange`，可选配 `x-dead-letter-routing-key`）。没有死信交换机的队列会丢弃它。

## 在本 crate 中 {/* #in-this-crate */}

队列类型是一个描述符类型，因为它决定了重试做什么：[`RabbitQueue`](api:ruststream_lapin::RabbitQueue) 对应经典队列，[`RabbitQuorumQueue`](api:ruststream_lapin::RabbitQuorumQueue) 对应仲裁队列。仲裁队列的描述符没有 `durable`、`exclusive` 和 `auto_delete` 步骤，因为仲裁队列总是持久的、共享的。

本 crate 用 `basic.reject` 结算否定的结果，从不使用 `basic.nack`。对单次投递，这两个帧做的事相同，而在仲裁队列上只有拒绝才计数。因此处理器的 `retry()` 会用掉消息的一次投递，队列的上限会结束一条一直失败的消息。在两种队列上，死信都由服务器自己移动：注册时的 `dead_letter` 会成为队列的死信路线，服务不发布副本。经典队列没有上限，所以不接受 `max_attempts`。[结算与重试](../settlement.md)用图说明了每种类型如何响应重试。

队列的规则来自 RabbitMQ（[经典队列](https://www.rabbitmq.com/docs/classic-queues)、[仲裁队列](https://www.rabbitmq.com/docs/quorum-queues)、[毒消息处理](https://www.rabbitmq.com/docs/quorum-queues#poison-message-handling)、[死信交换机](https://www.rabbitmq.com/docs/dlx)）。
