---
title: 工作分区与顺序
sidebar_position: 5
description: "一个订阅在一个信道上消费一个队列；workers(n, by_key) 按消息头把它的投递分散到多个工作分区。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "一个订阅的分发：一个队列、订阅的信道、一个流，以及按 amqp-partition-key 划分的两个工作分区",
  "text": "以文字阅读本图",
  "queue": ["队列 orders", "本服务的一个消费者"],
  "channel": ["订阅的信道", "未确认投递的预取窗口"],
  "stream": ["LapinSubscriber 流", "一次一条投递"],
  "keyed": ["workers(2, by_key)", "工作分区 = amqp-partition-key 消息头的哈希"],
  "lane0": ["工作分区 1", "键 a 和 c，", "各自有序"],
  "lane1": ["工作分区 2", "键 b，有序"],
  "hash": ["改在服务器上做：一致性哈希交换机", "把键分散到多个队列，每个队列一个订阅"],
  "pushed": "推送",
  "oneAtATime": "一次一条"
};

# 工作分区与顺序

一个订阅在一个信道上消费一个队列；`workers(n, by_key)` 按消息头把它的投递分散到多个工作分区。

<Diagram name="lanes" words={words} />

订阅的流一次产出一条投递，顺序就是队列推送它们的顺序。没有 `workers` 时，一个循环依次为每条投递运行处理器，所以队列的顺序得以保持，也没有任何东西并行运行。在此期间有多少条投递在服务中等待，由预取窗口决定。

`workers(n, by_key)` 运行 `n` 个工作分区。投递进入其键的哈希所对应的工作分区，一个工作分区按顺序处理它的投递。键是 [`amqp-partition-key`](api:ruststream_lapin::PARTITION_KEY_HEADER) 消息头，由生产者设置。AMQP 不读取它：它是生产者与本 crate 之间的约定。没有这个消息头的投递没有键，这类投递轮流进入各个工作分区。

工作分区需要大于 1 的预取窗口。预取值为 1 时，队列只在上一条投递结算后才推送下一条，各个工作分区只能轮流工作。

工作分区保持的是单个消费者内部的顺序。同一个队列上的两个服务实例是两个竞争消费者，队列会把同一个键的投递分散到这两个实例上。RabbitMQ 的单活跃消费者（`x-single-active-consumer`）是一个用 [`argument`](api:ruststream_lapin::RabbitQueue::argument) 设置的队列参数，它让同一时刻只有一个消费者读取队列。

[一致性哈希交换机](rabbitmq/routing.md)把同样的思路搬到服务器上。它把路由键分散到多个队列，每个队列有自己的订阅。正常运行时，一个键只进入一个队列，所以只要每个队列只有一个消费者，这个键的顺序就能跨实例保持。

[批处理器](rabbitmq/consumers.md)取走队列已经推送过来的投递，最多到挂载点指定的大小，所以一批往往小于这个大小。
