---
title: 请求与回复
sidebar_position: 7
description: "通过 RabbitMQ 调用另一个服务并等待它的回复：基于 direct reply-to 的请求方、回复它的应答方，以及超时。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 请求与回复

本指南让一个服务通过 RabbitMQ 调用另一个服务，并等待回复。

订单服务在接受订单之前，先询问库存服务某件商品是否有货。
调用经由 RabbitMQ 的 [direct reply-to](https://www.rabbitmq.com/docs/direct-reply-to) 进行：不需要声明回复队列，也不需要手工维护一张待处理调用表。

## 应答方 {/* #the-responder */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/rpc.rs#responder"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/rpc.rs#responder"
```

</TabItem>
</Tabs>

库存服务用一个返回回复的普通处理器作答。
回复发布者上的 [`DirectReplyTo`](../components/request/direct-reply-to.md) 把每条回复发送到请求在其 `reply-to` 属性中指定的地址，并把请求的 `correlation-id` 复制到回复上。
`Stock` 没有指定目的地，因为每条回复都发往其请求要求的地方。
`reply("inventory.check.unanswered")` 中的名字，是回复没有 `reply-to` 的请求时所用的地址，也是服务的 AsyncAPI 文档所报告的地址。

这些路由是一个 [`Router`](api:ruststream::runtime::Router)，所以库存服务在自己的应用里用 `b.include_router(inventory())` 挂载它们。
示例在一个进程里运行两个服务，并挂载两个路由器。

## 请求方 {/* #the-requester */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/rpc.rs#requester"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/rpc.rs#requester"
```

</TabItem>
</Tabs>

`place_order` 把它的槽位约束为 `Out<impl RequestReply>`，挂载点为它配上 [`Request`](../components/request/index.md) 策略，即 [`LapinRequest`](api:ruststream_lapin::LapinRequest)。
每个请求带着路由键 `inventory.check` 发往默认交换机，所以它到达同名的队列。
请求的 `reply-to` 设为 RabbitMQ 的 `amq.rabbitmq.reply-to`，并带有一个新生成的 `correlation-id`。
id 相同的回复完成这次调用；id 不同的回复被丢弃。
这里的请求和回复都是字节，所以 `ask_stock` 用 [`JsonCodec`](api:ruststream::codec::JsonCodec) 对它们编码和解码。

`RPC_TIMEOUT` 限定等待时间。
没有人及时应答时，调用返回 [`AmqpError::RequestTimeout`](../components/broker/errors.md) 错误。
这时 `place_order` 用 `retry_after` 要求 30 秒后再投递这个订单，而不是去猜测库存。
订单在队列的等待队列 `orders.retry` 中等待这几秒，该等待队列由 [`delay`](../components/settlement/delay.md) 提供。
商品缺货是业务上的答复，所以订单被丢弃。

这个策略接受发布所用的设置：[`exchange`](api:ruststream_lapin::LapinRequest::exchange)、`priority` 和 `expiration`。
请求默认是非持久化的：超时之后没人再等它，所以它在重启后存活下来也没有意义。
[`persistent(true)`](api:ruststream_lapin::LapinRequest::persistent) 让它持久化。

## 投递保证 {/* #delivery-guarantees */}

direct reply-to 是至多一次的。
回复地址存在于请求方位于某个 RabbitMQ 节点上的信道里，没有任何东西被持久地排入队列。
信道关闭时仍在途中的回复会丢失，调用方看到的是超时。
请求本身是普通队列里的一条普通消息：库存服务停机时，它在 `inventory.check` 中等待，可能在调用方放弃之后才得到应答。
所以库存查询可以安全地重复：应答两次不会改变任何东西。

请求方在自己的信道上读取回复，这个信道由它的第一个请求打开。

## 另一端使用其他技术栈 {/* #another-stack-on-the-other-side */}

双方说的都是普通的 AMQP 0.9.1。
用其他客户端编写的应答方消费请求队列，把回复发布到默认交换机，路由键取自 `reply-to`，并复制 `correlation-id`。
带 `DirectReplyTo` 的 RustStream 应答方也以同样的方式回复用其他客户端编写的请求方。

## 测试 {/* #testing */}

进程内 RabbitMQ 属于连接它的那个应用，所以请求方和应答方必须运行在同一个应用里才能通信。
测试启动示例中挂载了两个路由器的应用，检查有货的订单被接受、缺货的订单被丢弃。
第三个测试在暂停的时钟上只挂载订单路由器：没有人应答，请求超时，订单稍后再次询问。
进程内 RabbitMQ 像服务器一样为每个请求分配自己的回复地址，并按关联 id 匹配回复。
[测试 RabbitMQ 服务](testing.md)讲解了测试套件。
