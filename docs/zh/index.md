# RabbitMQ Broker

`ruststream-lapin` 通过 AMQP 0.9.1 客户端 [`lapin`](https://docs.rs/lapin) 把 RustStream 服务跑在
RabbitMQ 上。RabbitMQ 队列是销毁式存储：一次投递一经确认就离开队列。确认、重新入队和进死信都是
协议帧，因此框架在 Broker 本身上结算消息。

```toml
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-lapin = "0.7"
serde = { version = "1", features = ["derive"] }
```

`LapinBroker::new` 不做 I/O，所以 RabbitMQ 服务和其他 Broker 一样，用同一个 `#[ruststream::app]`
宏组装。运行时在启动时连接 Broker，然后才打开订阅。连接会消耗掉 Broker，交回
`ConnectedLapinBroker`，只有它带着订阅和发布的表面。

处理器主体点名能力：`Out<impl Publisher>`、`Out<impl TransactionalPublisher>`、
`Out<impl RequestReply>`。它只导入框架的 prelude，正因如此，路由文件给它配哪种发布策略，同一个
处理器都挂得上。有一种主体也导入本 crate 的 prelude：为单条消息调整某项
[AMQP 属性][properties]的那种。它点名的是设置类型而不是发布者类型
（`Out<impl Publisher<Options = LapinPublishOptions>>`），所以处理器照样不点名发布者。

路由文件点名值，导入 `ruststream_lapin::prelude::*`，框架的 prelude 随之一起进来。它再加上整个
家族统一的挂载点名字：`Publish`、`TransactionalPublish` 和 `Request`，分别是 `LapinPublish`、
`ConfirmsPublish` 和 `LapinRequest` 的别名。

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_quickstart.rs:handler"
```

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_quickstart.rs:app"
```

## 传输模型 { #the-transport-model }

- 一个订阅消费一个队列。`#[subscriber("orders")]` 消费名为 `orders` 的队列，
  [`RabbitQueue`][subscribing] 描述符再补上绑定、队列类型和预取。批量处理器在挂载点写出批次大小；
  AMQP 没有协议级的批次，所以 crate 在客户端攒批次，见[批次][batches]。
- 发布一侧，消息的名字就是路由键，交换机属于发布策略：不点名别的就走默认交换机。见
  [发布][publishing]。
- 结算是原生的：`ack` 发出 `basic.ack`，重试发出 `basic.reject(requeue = true)`，丢弃发出
  `basic.reject(requeue = false)`；队列配了死信交换机时，丢弃就把消息送进死信。RabbitMQ 把拒绝
  算作一次用掉的投递，所以处理器的重试会消耗仲裁队列自己的上限。
- 除非你用 `.declare_topology(true)` 显式开启，否则 Broker 上什么都不声明：拓扑由你自己管理。

## 能力 { #capabilities }

本 Broker 原生实现了框架的哪些可选能力 trait：

| 能力 | 原生 | 说明 |
| --- | --- | --- |
| `Subscribe` | 是 | 消费订阅点名的队列；[`RabbitQueue`](https://docs.rs/ruststream-lapin/latest/ruststream_lapin/index.html#subscribing) 再补上绑定、队列类型和预取。 |
| `BatchSubscriber` | 是，客户端攒批 | AMQP 一次只推一个 `basic.deliver`，所以批次在客户端攒出来，攒到挂载点写出的大小为止，受描述符的预取窗口和 `batch_wait` 约束。见[批次](https://docs.rs/ruststream-lapin/latest/ruststream_lapin/index.html#batches)。 |
| `TransactionalPublisher` | 是 | 两个事务发布者都支持：`.confirms()` 在客户端缓冲，提交时等齐每一条确认；`.server_tx()` 用 AMQP 的信道事务。见[发布](https://docs.rs/ruststream-lapin/latest/ruststream_lapin/index.html#publishing)。 |
| `OwnedTransactions` | 是（仅 confirms） | confirms 事务是一个客户端缓冲，所以同一个句柄上开多少个都行。`server_tx` 把信道本身切进事务模式，而那是信道状态，只存在一份。 |
| `RequestReply` | 是 | `LapinRequest` 构造出一个基于 direct reply-to 的请求方，按 correlation-id 多路复用。见[请求与回复](https://docs.rs/ruststream-lapin/latest/ruststream_lapin/index.html#request-and-reply)。 |
| `Partitioned` | 是 | 生产方把键写进 `amqp-partition-key` 消息头，运行时的工作分区读它；AMQP 本身不解释该消息头。见[投递元数据](https://docs.rs/ruststream-lapin/latest/ruststream_lapin/index.html#delivery-metadata)。 |
| `Seekable` + `Positioned` | 否 | 队列不保留历史，无处可回。 |
| `DescribeServer` | 是 | 报告连接的主机以及它背后的 AMQP 版本，AsyncAPI 文档记录的就是它。参见 [AsyncAPI 文档](https://docs.rs/ruststream-lapin/latest/ruststream_lapin/index.html#the-asyncapi-document)。 |

## 生成服务骨架 { #scaffold-a-service }

用 [`cargo generate`](https://github.com/cargo-generate/cargo-generate) 生成一个跑得起来的起步
项目，一种消息形态一个模板：

```bash
cargo generate --git https://github.com/powersemmi/ruststream-lapin templates/amqp-queue
cargo generate --git https://github.com/powersemmi/ruststream-lapin templates/amqp-topic
```

## 文档 { #documentation }

crate 在 docs.rs 上给自己写文档，那份概览就是指南：

- [订阅][subscribing] - 两个队列描述符、队列类型、绑定、预取、重试上限、延迟重新投递、批和
  投递元数据。
- [发布][publishing] - 路由模型、策略、单条消息的 AMQP 属性、回复和事务。
- [请求与回复][request-reply] - 基于 direct reply-to 的 RPC。
- [AsyncAPI 文档][documenting] - 服务对自己报告的 AMQP 绑定。
- [测试][testing] - `TestApp` 测试套件下的生产应用，在进程内运行，或对着运行中的 Broker。
- [运维][operations] - TLS、连接设置、按需声明和已知缺口。

安装、教程和其他 Broker 在 RustStream 站点上：
<https://powersemmi.github.io/ruststream/>。

[subscribing]: https://docs.rs/ruststream-lapin/latest/ruststream_lapin/index.html#subscribing
[batches]: https://docs.rs/ruststream-lapin/latest/ruststream_lapin/index.html#batches
[publishing]: https://docs.rs/ruststream-lapin/latest/ruststream_lapin/index.html#publishing
[properties]: https://docs.rs/ruststream-lapin/latest/ruststream_lapin/index.html#per-message-properties
[request-reply]: https://docs.rs/ruststream-lapin/latest/ruststream_lapin/index.html#request-and-reply
[documenting]: https://docs.rs/ruststream-lapin/latest/ruststream_lapin/index.html#the-asyncapi-document
[testing]: https://docs.rs/ruststream-lapin/latest/ruststream_lapin/index.html#testing
[operations]: https://docs.rs/ruststream-lapin/latest/ruststream_lapin/index.html#operations
