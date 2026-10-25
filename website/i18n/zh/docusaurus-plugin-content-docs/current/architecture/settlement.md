---
title: 结算与重试
sidebar_position: 6
description: "ack、drop、retry 和 retry_after 在 RabbitMQ 上变成什么；在这里，每次结算都是投递所在信道上的一个帧。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const retries = {
  "title": "重试步骤下的一次重试与一次丢弃：经典队列只接受 dead_letter，仲裁队列在服务器上计数",
  "text": "以文字阅读本图",
  "outcome": ["处理器返回 retry() 或 drop()", "且声明了重试步骤"],
  "classic": ["RabbitQueue：", "只有 dead_letter，", "不计数"],
  "quorum": ["RabbitQuorumQueue：", "服务器计数，记在", "x-delivery-count"],
  "classicRequeue": ["retry()：", "拒绝并", "重新入队"],
  "classicDropped": ["drop()：", "服务器将", "其转为", "死信"],
  "requeue": ["低于上限：", "拒绝并", "重新入队"],
  "quorumSpent": ["超过上限：", "服务器将", "其转为", "死信"],
  "arrives": ["回到队列中", "等待再次投递"],
  "plain": ["什么都未声明时，retry() 在两种队列上", "都是带 requeue 的 basic.reject"],
  "below": "低于上限"
};

export const delays = {
  "title": "延迟重试在 TTL 等待队列或延迟消息交换机中等待；两者都没有的队列立即重新入队",
  "text": "以文字阅读本图",
  "outcome": ["处理器返回", "retry_after(delay)"],
  "immediate": ["未设置 delay：", "立即重新入队，", "延迟被舍弃"],
  "waitingQueue": ["Delay::dlx_ttl：", "<queue>.retry", "按单条消息", "设置 TTL"],
  "pluginExchange": ["Delay::", "plugin_dme：", "插件交换机上", "的 x-delay"],
  "origin": ["投递所来自的队列", "副本是一条新消息"],
  "restart": ["Broker 上的副本比服务活得更久；", "服务自身不持有副本"],
  "requeued": "重新入队",
  "released": "释放"
};

# 结算与重试

`ack`、`drop`、`retry` 和 `retry_after` 在 RabbitMQ 上变成什么；在这里，每次结算都是投递所在信道上的一个帧。

| 结果 | [`RabbitQueue`](api:ruststream_lapin::RabbitQueue)（经典队列） | [`RabbitQuorumQueue`](api:ruststream_lapin::RabbitQuorumQueue) |
|---|---|---|
| `ack` | `basic.ack`：消息离开队列 | 相同 |
| `drop` | 不带 `requeue` 的 `basic.reject`：队列有死信交换机时转为死信，否则丢弃 | 相同 |
| `retry()` | 带 `requeue` 的 `basic.reject`：回到队列，不计数 | 带 `requeue` 的 `basic.reject`：回到队列，服务器计入这次投递 |
| 在 `max_attempts` 和 `dead_letter` 下的 `retry()` | 启动时被拒绝：没有谁计数 | 服务器自己的投递上限，见下文 |
| 在 `dead_letter` 下的 `drop` | 服务器把消息转为死信，送往 `dead_letter` 队列 | 相同 |
| `retry_after(delay)` | 有 `delay` 时是一份等待的副本，[见下文](#delayed-retries)；没有时立即重新入队 | 相同 |

本 crate 从不发送 `basic.nack`。对单次投递，它做的事与 `basic.reject` 相同，而从 RabbitMQ 4.3 起，只有拒绝才计入仲裁队列的投递上限。批处理器的结果用同一个帧结算这一批中的每次投递。经由[请求方](rabbitmq/reply-to.md)到达的回复不需要结算：它到达的是一个不需要确认的消费者。

## 重试 {/* #retries */}

<Diagram name="retries" words={retries} />

挂载点上的 `max_attempts(n)` 和 `dead_letter(name)` 结束一条一直失败的消息。第一次投递算作一次尝试。两者都会成为队列的参数，所以由服务器移动消息，服务自己不发布副本。

经典队列什么都不统计，所以只接受 `dead_letter`。目的地成为该队列的死信路线：`x-dead-letter-exchange` 是默认交换机，`x-dead-letter-routing-key` 是目的地；描述符指定了自己的交换机时除外。`drop` 以不带 `requeue` 的方式拒绝投递，服务器把它移到那里。`retry` 是普通的重新入队，不计数。在经典队列上使用 `max_attempts` 会让启动失败，错误信息会指向仲裁队列。

仲裁队列自己统计投递次数。这一对设置变成队列的 `x-delivery-limit`（`max_attempts` 减一，因为服务器统计的是退回次数），以及通往 `dead_letter` 队列的死信路线。重试是普通的带 `requeue` 的 `basic.reject`，消息超过上限后，服务器把它转为死信，无论服务是否在运行。这一对设置需要 [`declare_topology(true)`](api:ruststream_lapin::LapinBroker::declare_topology)，没有它启动就会失败。

本服务声明队列时，参数才会写到队列上。如果队列已经以其他参数存在，服务器会以参数不等价为由拒绝声明，启动失败，错误中列出该队列和发送的参数。本服务不声明的经典队列保留运维人员为它设置的路线。只给出队列名时不带任何参数，所以两个步骤都不接受。两种队列上都没有副本离开本进程，所以重试发布者（`out_retry`）无法通过编译。

## 延迟重试 {/* #delayed-retries */}

<Diagram name="delays" words={delays} />

`retry_after(delay)` 要求在不早于 `delay` 之后再投递一次。AMQP 没有单条消息的延迟，所以等待发生在 Broker 上，在两个地方之一。

队列上没有 [`delay`](api:ruststream_lapin::RabbitQueue::delay) 时，什么都不等待：投递立即重新入队，并记录一条警告，说明延迟被舍弃了。

[`Delay::dlx_ttl`](api:ruststream_lapin::Delay::dlx_ttl) 把副本留在 Broker 上。本 crate 把它发布到一个等待队列（默认为 `<queue>.retry`），以延迟作为单条消息的过期时间，然后在同一信道上确认原始消息。过期时间一到，等待队列把副本作为死信送回原队列。队列只从队头释放过期消息，所以排在长延迟后面的短延迟要等长延迟到期；每种延迟用一个等待队列，就能把它们分开。

[`Delay::plugin_dme`](api:ruststream_lapin::Delay::plugin_dme) 在 `plugin-dme` feature 下可用，它把副本发布到延迟消息插件的一个交换机（默认为 `<queue>.delay`），延迟放在 `x-delay` 消息头中。延迟过去后，插件把它路由到原队列，不同长度的延迟互不等待。

两种目标都属于基础设施。在 `declare_topology(true)` 下，每个订阅声明自己的目标。不做声明时，订阅在启动时检查目标是否存在，缺少时启动失败，因为服务器会丢弃发往不存在的等待队列的副本。副本是一条新消息，所以服务器从零开始为它计数，仲裁队列的上限只结束立即重试。它带着加一后的 `x-ruststream-retry-count` 消息头。

服务器一侧的规则来自 RabbitMQ（[否定确认](https://www.rabbitmq.com/docs/confirms)、[仲裁队列的投递上限](https://www.rabbitmq.com/docs/quorum-queues#poison-message-handling)、[死信](https://www.rabbitmq.com/docs/dlx)、[单条消息的 TTL](https://www.rabbitmq.com/docs/ttl)、[延迟消息交换机](https://github.com/rabbitmq/rabbitmq-delayed-message-exchange)）。
