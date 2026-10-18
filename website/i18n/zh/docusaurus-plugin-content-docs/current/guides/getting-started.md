---
title: 快速开始
sidebar_position: 1
description: "安装 ruststream-lapin，启动本地 RabbitMQ，运行第一个订阅者，并在没有服务器的情况下测试它。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 快速开始

本指南安装 `ruststream-lapin`，启动一个本地 RabbitMQ，并让第一个服务连上它运行。

## 安装 {/* #installation */}

```toml title="Cargo.toml"
[dependencies]
ruststream = { version = "0.7", features = ["macros", "json", "asyncapi", "logging"] }
ruststream-lapin = "0.7"
serde = { version = "1", features = ["derive"] }
tracing = "0.1"

[dev-dependencies]
ruststream-lapin = { version = "0.7", features = ["testing"] }
tokio = { version = "1", features = ["macros", "rt"] }
```

`ruststream` 是框架本身：`macros` 提供属性宏，`json` 提供默认编解码器，`asyncapi` 生成服务的 AsyncAPI 文档，`logging` 把服务的日志打印到终端。
`ruststream-lapin` 是 RabbitMQ Broker，基于 [lapin](https://docs.rs/lapin) 构建，lapin 是用 Rust 编写的 AMQP 0.9.1 客户端。
构建只需要 Cargo。
它的 `testing` feature 加入测试所用的[进程内 RabbitMQ](../components/testing/index.md)。

这个 Broker crate 有以下 feature：

| Feature | 提供的内容 |
|---|---|
| `asyncapi` | 生成的 AsyncAPI 文档中的 [AMQP 绑定](../components/asyncapi/index.md) |
| `testing` | `TestApp` 背后的进程内 RabbitMQ |
| `tls-native-tls` | 通过平台自带的 TLS 库提供 [TLS](../components/broker/tls.md) |
| `tls-rustls` | 通过 rustls 提供 TLS |
| `tls-rustls-ring` | 通过使用 `ring` 加密后端的 rustls 提供 TLS |
| `plugin-consistent-hash` | RabbitMQ 插件提供的[一致性哈希交换机](../components/exchanges/consistent-hash.md) |
| `plugin-dme` | 借助延迟消息交换机插件实现的[延迟重试](../components/settlement/plugin-dme.md) |

## 本地 RabbitMQ {/* #a-local-rabbitmq */}

开发时，一个带管理界面的 RabbitMQ 节点就够了：

```yaml title="compose.yaml"
services:
  rabbitmq:
    image: rabbitmq:4-management-alpine
    ports:
      - "127.0.0.1:5672:5672"
      - "127.0.0.1:15672:15672"
```

```bash
docker compose up -d
```

Broker 监听 `localhost:5672`。
管理界面位于 `http://localhost:15672`，用户名 `guest`，密码 `guest`。
RabbitMQ 只接受来自 localhost 的 `guest` 账户，所以端口只发布在本机地址上。

## 第一个服务 {/* #the-first-service */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/getting_started.rs#service"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/getting_started.rs#service"
```

</TabItem>
</Tabs>

`take_order` 从 `orders` 队列读取 JSON 订单。
带金额的订单被确认，没有金额的订单被丢弃。
[`LapinBroker::new`](../components/broker/index.md) 接收连接 URI，只把它记录下来。
连接在服务启动时才打开。
[`declare_topology(true)`](../components/broker/declare-topology.md) 让服务自己声明它读取的队列，所以服务可以在空的 RabbitMQ 上启动。
`#[ruststream::app]` 把 `app` 变成 `main`；在手写形式中，[`run_main`](api:ruststream::runtime::cli::run_main) 做同样的事。

运行服务：

```bash
cargo run -- run
```

在另一个终端里，用镜像自带的 `rabbitmqadmin` 发布一个订单：

```bash
docker compose exec rabbitmq rabbitmqadmin publish message \
  --routing-key orders --payload '{"id":1,"amount_cents":1250}'
```

消息进入默认交换机，默认交换机把它投递到与路由键同名的队列。
服务记录一条带订单 id 的 `order taken` 日志。
在管理界面里，`orders` 队列的页面也能发布同样的消息。
<kbd>Ctrl</kbd>+<kbd>C</kbd> 停止服务。

服务还能描述自己：

```bash
cargo run -- asyncapi gen
```

这条命令打印服务的 AsyncAPI 文档：服务器、`orders` 通道和 `Order` 消息。

`cargo generate` 可以生成同类服务的脚手架，其中带有路由器和回复：

```bash
cargo generate --git https://github.com/powersemmi/ruststream-lapin templates/amqp-queue --name my-service
```

`templates/amqp-topic` 模板则从一个 topic 交换机开始。

## 第一个测试 {/* #the-first-test */}

示例文件里还有两个测试。
每个测试都用 `TestApp::start` 启动同一个 `app`，发布一个订单，然后检查 `take_order` 如何结算它。
在测试中，`LapinBroker` 连接的是进程内 RabbitMQ，所以测试不需要服务器：

```bash
cargo test
```

[测试 RabbitMQ 服务](testing.md)逐一讲解这些测试，以及进程内 RabbitMQ 模拟了什么。

## 从 lapin 迁移 {/* #coming-from-lapin */}

基于 lapin 的服务要自己打开信道，自己编写消费者循环。
下表列出每一部分在 RustStream 中的位置：

| lapin | RustStream |
|---|---|
| `Connection::connect` 和 `create_channel` | [`LapinBroker::new`](../components/broker/connection.md)；运行时在启动时建立连接 |
| `channel.basic_qos` | Broker 上或单个队列上的 [`prefetch`](../components/subscriptions/prefetch.md) |
| `queue_declare`、`exchange_declare`、`queue_bind` | [队列描述符](../components/subscriptions/rabbit-queue.md)和 [`declare_topology(true)`](../components/broker/declare-topology.md)（[拓扑](topology.md)） |
| `basic_consume` 加一个 `while let Some(delivery)` 循环 | 一个 `#[subscriber]` 处理器 |
| 反序列化 `delivery.data` | 处理器的参数类型，由编解码器解码 |
| `delivery.ack` 和 `delivery.reject` | 处理器的[结果](../components/settlement/index.md)（[确认、重试与死信](retries.md)） |
| `basic_publish` | 回复或 `Out` 参数，经由[发布策略](../components/publishing/index.md)发送 |
| `confirm_select` 并等待确认 | [`ConfirmsPublish`](../components/transactions/confirms-publish.md)（[可靠发布](publishing.md)） |
| 回调队列和关联表 | [`LapinRequest`](../components/request/index.md)（[请求与回复](rpc.md)） |
