---
title: 交换机类型
sidebar_position: 1
description: "每种交换机类型读取消息的不同部分来选择队列：路由键、路由键上的模式、什么都不读，或者消息头。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "一条路由键为 order.eu.placed、带消息头 region=eu 的消息交给五种交换机类型，以及每种类型选出的队列",
  "text": "以文字阅读本图",
  "message": ["一条消息：路由键 order.eu.placed，", "消息头 region = eu"],
  "exchanges": "交换机类型",
  "direct": ["direct", "路由键等于绑定键"],
  "directQueues": ["以 order.eu.placed", "绑定的队列"],
  "topic": ["topic", "路由键匹配模式"],
  "topicQueues": ["以 order.*.placed 或", "order.# 绑定的队列"],
  "fanout": ["fanout", "忽略路由键"],
  "fanoutQueues": ["绑定到它的", "每个队列"],
  "headers": ["headers", "x-match = all 或 any"],
  "headersQueues": ["绑定要求", "region = eu 的队列"],
  "hash": ["x-consistent-hash", "插件：路由键的哈希"],
  "hashQueues": ["一个队列，按其", "权重比例选出"],
  "offered": "发布到"
};

# 交换机类型

每种交换机类型读取消息的不同部分来选择队列：路由键、路由键上的模式、什么都不读，或者消息头。

<Diagram name="exchanges" words={words} />

`direct` 交换机把消息复制到绑定键与其路由键相等的每个队列。默认交换机就是一个 `direct` 交换机，每个队列都以自己的名字绑定到它。

`topic` 交换机把路由键看作以点分隔的词。绑定是这些词上的一个模式：`*` 恰好匹配一个词，`#` 匹配零个或多个词。路由键 `order.eu.placed` 匹配 `order.*.placed` 和 `order.#`，不匹配 `order.*`。

`fanout` 交换机忽略路由键，把消息复制到绑定它的每个队列。

`headers` 交换机同样忽略路由键。它把消息的消息头与绑定的参数比较。`x-match` 设为 `all` 时，每个参数都必须匹配；设为 `any` 时，匹配一个就够了。在本 crate 中，带参数的绑定是 [`bind_with`](api:ruststream_lapin::RabbitQueue::bind_with)。

一条消息可以匹配多个绑定。这时它到达每个匹配的队列，每个队列一次。

一致性哈希交换机来自 RabbitMQ 自带的一个插件，需要启用。它对路由键求哈希，把消息发往一个绑定的队列。绑定的键就是它的权重：以权重 2 绑定的队列得到的键，大约是以权重 1 绑定的队列的两倍。正常运行时，路由键相同的消息进入同一个队列；节点重启可能把某个键移到另一个队列。本 crate 在 `plugin-consistent-hash` feature 下以 [`RabbitExchange::consistent_hash`](api:ruststream_lapin::RabbitExchange::consistent_hash) 提供它，[工作分区与顺序](../lanes.md)把它与单个订阅内部的工作分区做了比较。

[`RabbitExchange`](api:ruststream_lapin::RabbitExchange) 指定绑定所用的交换机。[`custom`](api:ruststream_lapin::RabbitExchange::custom) 指定插件添加的其他任何类型。

路由规则来自 RabbitMQ（[交换机](https://www.rabbitmq.com/docs/exchanges)、[topic 交换机](https://www.rabbitmq.com/tutorials/tutorial-five-python)、[一致性哈希交换机](https://github.com/rabbitmq/rabbitmq-server/tree/main/deps/rabbitmq_consistent_hash_exchange)）。
