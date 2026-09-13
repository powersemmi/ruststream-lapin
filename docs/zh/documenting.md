# AsyncAPI 文档

由 `#[ruststream::app]` 生成的服务，用 `cargo run -- asyncapi gen` 打印自己的 AsyncAPI 文档。频道、
消息和它们的 schema 由框架写；这个 crate 填的是其中属于 AMQP 的那一半，位于一个特性之后：

```toml
ruststream-lapin = { version = "0.7", features = ["asyncapi"] }
```

不开这个特性，文档照样生成，只是里面没有任何 RabbitMQ 特有的东西。开了之后，一个读取一个队列、
用 direct reply-to 作答的服务会这样报告自己：

```json
--8<-- "crates/ruststream-lapin/tests/snapshots/asyncapi-amqp.json"
```

## 服务器

`host` 与 `protocol` 说明客户端连到哪里、说的是什么，`protocolVersion` 则说明那是哪一种 AMQP。这个
区分不是装饰：AMQP 1.0 是同名的另一个协议，绑定键不同，客户端也不同，而主机地址分不出这两者。

文档会被发布和传阅，所以 AMQP 地址里的凭据不会进来。用 `amqp://svc:secret@rabbit:5672/prod` 建出的
Broker，把自己描述为 `rabbit:5672` - 密码和虚拟主机都被丢掉。

## 订阅读取的频道

订阅的频道是一个队列，队列名就是频道地址。`amqp` 绑定带的是描述符对这个队列的说法：它是否挺过
重启、是否为单个连接独占、是否随最后一个消费者一起删除。

队列绑定到的交换机在这里没有位置。规范里的频道绑定要么是队列，要么是路由键，而这个频道的地址
是队列。

## 接收操作

接收操作上的 `ack: true` 表示消费者自己确认。这个 crate 一向如此：一次投递由处理器的结果来结算，
而不是由 Broker 发出它这件事本身。

## 发布者写入的频道

发布策略从另一侧描述频道。`LapinPublish::default().exchange("x")` 是交换机 `x` 上的一个路由键，
所以绑定报告 `is: routingKey` 和该交换机的名字。在默认交换机上，路由键就是队列名，绑定也就照直
写成 `is: queue`。

发送操作带的是每条经由该策略的消息所取得的属性：`deliveryMode`（持久化默认值为 2，
`persistent(false)` 为 1），以及策略固定下来的 `priority` 与 `expiration`。为单条消息调整属性的
那次发布不会改动文档：文档描述的是声明，不是调用。

应答是那个例外，它没有自己的发送操作，所以应答策略的操作绑定进不了任何文档。频道绑定进得去。

## 客户端从哪里读应答地址

挂载了 `DirectReplyTo` 的处理器，把每个请求答到请求自己指定的地方，所以应答频道没有地址可报。
操作转而点名读取地址的那个消息头 - `$message.header#/reply-to`，也就是写成运行期表达式的
direct reply-to 约定。

不带这个变换时，应答去往挂载点声明的名字，频道报告的也正是这个名字。

## 哪些留空

规范的 AMQP 绑定里有两个字段，在这里没有诚实的来源。虚拟主机住在 Broker 的连接地址里，而描述符和
策略都看不到它。消息类型是 Rust 类型的名字，文档已经把它作为消息自己的名字报出来了，而绑定钩子
也学不到它：交给钩子的是描述符，不是消息类型。
