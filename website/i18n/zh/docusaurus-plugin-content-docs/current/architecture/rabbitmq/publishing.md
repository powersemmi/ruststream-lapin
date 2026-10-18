---
title: 发布者确认与事务
sidebar_position: 4
description: "向 RabbitMQ 的一次发布，在帧写出时、服务器确认时或信道事务提交时完成。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "发布完成的三种方式：发后即忘在帧写出时，发布者确认在服务器应答时，事务在提交时",
  "text": "以文字阅读本图",
  "fireAndForget": "发后即忘",
  "confirms": "发布者确认",
  "transaction": "事务",
  "ffPublish": "basic.publish",
  "ffDone": ["帧写出", "后立即", "返回"],
  "confirmSelect": ["confirm.select", "一次"],
  "confirmPublish": "basic.publish",
  "confirmAck": ["每条消息", "收到 basic.ack", "或 basic.nack"],
  "txSelect": ["tx.select", "一次"],
  "txPublish": ["basic.publish", "一次或多次"],
  "txCommit": "tx.commit",
  "txVisible": ["commit-ok：", "每次发布", "都可见"],
  "unroutable": ["没有队列接收的消息会被确认并丢弃：", "本 crate 发布时不带 mandatory 标志"]
};

# 发布者确认与事务

向 RabbitMQ 的一次发布，在帧写出时、服务器确认时或信道事务提交时完成。

<Diagram name="publishing" words={words} />

`basic.publish` 本身没有应答。发送后就继续往下走的发布者对这条消息一无所知：不存在的交换机或出了故障的队列都不会被察觉。

发布者确认补上了这个应答。`confirm.select` 之后，信道为它的发布编号，服务器在承担起每条消息的责任后，用 `basic.ack` 应答。对持久队列中的持久化消息，这意味着已写入磁盘；对仲裁队列，这意味着已被法定数量的副本接受。`basic.nack` 表示服务器无法处理这条消息，这种情况很少见：它来自队列的内部错误。

不匹配任何绑定的消息也会被确认。交换机找不到队列，服务器确认这条消息，然后丢弃它。只有 `mandatory` 标志才会让服务器把这样的消息退回给发布者。

信道事务在服务器上暂存发布。`tx.select` 之后，发布一直等到 `tx.commit`；`commit-ok` 表示其中每一次发布都已可见，`tx.rollback` 则丢弃它们。每次提交都是一次等待服务器的往返。节点在提交过程中故障时，RabbitMQ 不保证原子性。

## 在本 crate 中 {/* #in-this-crate */}

发布策略决定模式，更强的模式就是另一个策略：

| 策略 | 信道 | 发布何时返回 |
|---|---|---|
| [`LapinPublish`](api:ruststream_lapin::LapinPublish) | 连接上所有这类发布者共用的一个信道 | 帧写出之后 |
| [`ConfirmsPublish`](api:ruststream_lapin::ConfirmsPublish) | 自己的信道，处于确认模式 | 服务器确认之后；否定确认是一个错误 |
| [`ServerTxPublish`](api:ruststream_lapin::ServerTxPublish) | 自己的信道，处于事务模式 | 在事务内，提交时 |

发布者确认或事务信道在第一次发布时打开，所以从不发布的策略不持有信道。每次发布都不带 `mandatory`，所以已确认的发布仍可能没有到达任何队列：确认表示服务器收下了消息，而不是有队列持有它。除非策略另行指定，消息都是持久化的。

信道错误会关闭信道，连接保持不变。一次发往不存在的交换机的发后即忘发布就足以触发它，而发布者看不到服务器的应答。下一次发布会打开一个新信道。带着未完成的事务而失效的事务信道是个例外：事务存在于这个信道上，所以提交会失败，而不是报告成功。

[`ConfirmsPublish`](api:ruststream_lapin::ConfirmsPublish) 的事务是服务中的一个缓冲区。提交按顺序发布缓冲的消息，并等待每一个确认。它对每条消息是持久的，但不是原子的：在失败的那条之前发出的消息仍然是已发布的。[`ServerTxPublish`](api:ruststream_lapin::ServerTxPublish) 的事务是 RabbitMQ 自己的事务。每个信道只有一个，因为 `tx.select` 让信道本身进入事务模式。

协议规则来自 RabbitMQ（[发布者确认](https://www.rabbitmq.com/docs/confirms)、[事务](https://www.rabbitmq.com/docs/semantics)）。
