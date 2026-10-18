---
title: 模型
sidebar_position: 0
description: "RabbitMQ 让消息经由交换机按绑定进入队列，消费者通过连接中的一个信道读取一个队列。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "生产者发布到交换机，绑定把消息复制到队列，消费者读取队列；客户端通过同一个连接中的多个信道通信",
  "text": "以文字阅读本图",
  "producer": ["生产者", "带着路由键发布"],
  "vhost": "虚拟主机",
  "exchange": ["交换机", "按绑定选出队列"],
  "queue": ["队列", "保存消息，直到消费者结算"],
  "consumer": ["消费者", "读取一个队列"],
  "connection": "一个 TCP 连接",
  "channel1": "信道 1",
  "channel2": "信道 2",
  "channelN": "信道 n",
  "channels": ["每次发布和每个消费者都运行在一个信道上：", "信道是连接复用的轻量会话"],
  "publishes": "发布",
  "binds": "绑定",
  "delivers": "投递"
};

# 模型

RabbitMQ 让消息经由交换机按绑定进入队列，消费者通过连接中的一个信道读取一个队列。

<Diagram name="model" words={words} />

生产者从不直接写入队列。它发布到交换机，并为消息指定一个路由键。交换机把消息与自己的绑定逐一比较，绑定相匹配的每个队列都得到一份副本。不匹配任何绑定的消息不会到达任何队列。

队列是由消费者取空的存储。消费者确认消息后，消息就离开队列。在此之前，服务器一直保存它；消费者退回的投递或从未结算的投递，会回到队列。

每台服务器都有一个名字为空的默认交换机。每个队列都以自己的名字绑定到它，所以以路由键 `orders` 发布到默认交换机的消息会落入 `orders` 队列。没有自己拓扑的服务，正是这样仍然能发布到队列。

交换机、队列和绑定都位于一个虚拟主机中，连接 URI 指定这个虚拟主机。客户端打开一个 TCP 连接，在其中的信道上完成工作。信道是一个有自身状态的轻量会话：预取窗口、确认模式、事务。信道上的错误会关闭这个信道，连接保持不变。

这个模型就是 RabbitMQ 实现的 AMQP 0.9.1（[AMQP 概念](https://www.rabbitmq.com/tutorials/amqp-concepts)、[信道](https://www.rabbitmq.com/docs/channels)、[默认交换机](https://www.rabbitmq.com/docs/exchanges#default-exchange)）。每种交换机类型如何路由，见[交换机类型](routing.md)；队列如何向消费者推送，见[消费者与预取](consumers.md)；两种队列类型有何不同，见[经典队列与仲裁队列](queues.md)。
