---
title: 结算与重试
sidebar_position: 6
description: "ack、drop、retry 和 retry_after 在 RabbitMQ 上变成什么；在这里，每次结算都是投递所在信道上的一个帧。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const retries = {
  "title": "max_attempts 和 dead_letter 下的一次重试：经典队列把计数交给运行时，仲裁队列在服务器上计数",
  "text": "以文字阅读本图",
  "outcome": ["处理器返回 retry()", "且声明了 max_attempts 和 dead_letter"],
  "classic": ["RabbitQueue：", "运行时计数，记在", "x-ruststream-retry-count"],
  "quorum": ["RabbitQuorumQueue：", "服务器计数，记在", "x-delivery-count"],
  "copy": ["低于上限：", "副本发往", "原队列"],
  "classicSpent": ["到达上限：", "副本发往", "死信", "队列"],
  "requeue": ["低于上限：", "拒绝并", "重新入队"],
  "quorumSpent": ["超过上限：", "服务器将", "其转为", "死信"],
  "arrives": ["回到队列中", "等待再次投递"],
  "plain": ["两者都未声明时，retry() 在两种队列上", "都是带 requeue 的 basic.reject"],
  "below": "低于上限"
};

export const delays = {
  "title": "延迟重试在服务进程中、在 TTL 等待队列中或在延迟消息交换机中等待，每种方式最后都把副本送回原队列",
  "text": "以文字阅读本图",
  "outcome": ["处理器返回", "retry_after(delay)"],
  "process": ["未设置 delay：", "服务进程", "中的计时器"],
  "waitingQueue": ["Delay::dlx_ttl：", "<queue>.retry", "按单条消息", "设置 TTL"],
  "pluginExchange": ["Delay::", "plugin_dme：", "插件交换机上", "的 x-delay"],
  "origin": ["投递所来自的队列", "副本是一条新消息"],
  "restart": ["进程退出时，进程中的计时器随之丢失；", "Broker 上的副本比服务活得更久"],
  "republished": "重新发布",
  "released": "释放"
};

# 结算与重试

`ack`、`drop`、`retry` 和 `retry_after` 在 RabbitMQ 上变成什么；在这里，每次结算都是投递所在信道上的一个帧。

| 结果 | [`RabbitQueue`](api:ruststream_lapin::RabbitQueue)（经典队列） | [`RabbitQuorumQueue`](api:ruststream_lapin::RabbitQuorumQueue) |
|---|---|---|
| `ack` | `basic.ack`：消息离开队列 | 相同 |
| `drop` | 不带 `requeue` 的 `basic.reject`：队列有死信交换机时转为死信，否则丢弃 | 相同 |
| `retry()` | 带 `requeue` 的 `basic.reject`：回到队列，不计数 | 带 `requeue` 的 `basic.reject`：回到队列，服务器计入这次投递 |
| 在 `max_attempts` 和 `dead_letter` 下的 `retry()` | 运行时发布一份计数的副本，见下文 | 服务器自己的投递上限，见下文 |
| `retry_after(delay)` | 一份等待的副本，[见下文](#delayed-retries) | 有 `delay` 时相同；没有时立即重新入队 |

本 crate 从不发送 `basic.nack`。对单次投递，它做的事与 `basic.reject` 相同，而从 RabbitMQ 4.3 起，只有拒绝才计入仲裁队列的投递上限。批处理器的结果用同一个帧结算这一批中的每次投递。经由[请求方](rabbitmq/reply-to.md)到达的回复不需要结算：它到达的是一个不需要确认的消费者。

## 重试 {/* #retries */}

<Diagram name="retries" words={retries} />

挂载点上的 `max_attempts(n)` 和 `dead_letter(name)` 结束一条一直失败的消息。第一次投递算作一次尝试。由谁计数取决于队列类型。

经典队列什么都不统计，所以由运行时计数。低于上限时，重试会以不带 `requeue` 的方式拒绝原始消息，并以队列的名字把一份副本发布到默认交换机，副本落在同一个队列的末尾。副本在 [`x-ruststream-retry-count`](api:ruststream::runtime::RETRY_COUNT_HEADER) 消息头中带着尝试序号。到达上限时，副本改为发往 `dead_letter` 指定的队列；没有 `dead_letter` 时，投递被拒绝，不发布任何东西。如果经典队列有自己的死信交换机，每条原始消息的拒绝也会到达那个交换机。

仲裁队列自己统计投递次数。这一对设置变成队列的 `x-delivery-limit`（`max_attempts` 减一，因为服务器统计的是退回次数），以及通往 `dead_letter` 队列的死信路线。重试是普通的带 `requeue` 的 `basic.reject`，消息超过上限后，服务器把它转为死信，无论服务是否在运行。这些参数只有在本服务声明队列时才会写到队列上，所以这一对设置需要 [`declare_topology(true)`](api:ruststream_lapin::LapinBroker::declare_topology)，没有它启动就会失败。仲裁队列不发布副本，所以重试发布者（`out_retry`）在它上面无法通过编译。

## 延迟重试 {/* #delayed-retries */}

<Diagram name="delays" words={delays} />

`retry_after(delay)` 要求在不早于 `delay` 之后再投递一次。AMQP 没有单条消息的延迟，所以等待发生在三个地方之一。

经典队列上没有 [`delay`](api:ruststream_lapin::RabbitQueue::delay) 时，由运行时持有副本。它拒绝原始消息，并在服务中的计时器触发时发布副本。优雅停机会等待计时器，而在计时器触发前退出的进程会丢失副本。仲裁队列上没有 `delay` 时，运行时没有可持有的副本：投递立即重新入队，并记录一条警告，说明延迟被舍弃了。

[`Delay::dlx_ttl`](api:ruststream_lapin::Delay::dlx_ttl) 把副本留在 Broker 上。本 crate 把它发布到一个等待队列（默认为 `<queue>.retry`），以延迟作为单条消息的过期时间，然后在同一信道上确认原始消息。过期时间一到，等待队列把副本作为死信送回原队列。队列只从队头释放过期消息，所以排在长延迟后面的短延迟要等长延迟到期；每种延迟用一个等待队列，就能把它们分开。

[`Delay::plugin_dme`](api:ruststream_lapin::Delay::plugin_dme) 在 `plugin-dme` feature 下可用，它把副本发布到延迟消息插件的一个交换机（默认为 `<queue>.delay`），延迟放在 `x-delay` 消息头中。延迟过去后，插件把它路由到原队列，不同长度的延迟互不等待。

两种目标都属于基础设施。在 `declare_topology(true)` 下，每个订阅声明自己的目标。不做声明时，订阅在启动时检查目标是否存在，缺少时启动失败，因为服务器会丢弃发往不存在的等待队列的副本。副本是一条新消息，所以服务器从零开始为它计数。它带着加一后的 `x-ruststream-retry-count` 消息头；在经典队列上，运行时在发送副本之前从中读取上限：到达上限的投递进入死信队列，而不是再次等待。

服务器一侧的规则来自 RabbitMQ（[否定确认](https://www.rabbitmq.com/docs/confirms)、[仲裁队列的投递上限](https://www.rabbitmq.com/docs/quorum-queues#poison-message-handling)、[死信](https://www.rabbitmq.com/docs/dlx)、[单条消息的 TTL](https://www.rabbitmq.com/docs/ttl)、[延迟消息交换机](https://github.com/rabbitmq/rabbitmq-delayed-message-exchange)）。
