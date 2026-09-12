# 队列与拓扑

`RabbitQueue` 描述符点名处理器消费的队列，并写出该队列应有的样子：持久化、队列类型、交换机
绑定、预取，以及原始的 `x-*` 参数。描述符直接写在 `#[subscriber(..)]` 属性里：

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_topology.rs:descriptor"
```

裸字符串形态 `#[subscriber("orders")]` 是 `RabbitQueue::new("orders")` 的简写：一个持久化、非
独占、不自动删除的队列，照原样消费。

## 声明是按需开启的 { #declaration-is-an-opt-in }

描述符写出订阅期待的拓扑。默认情况下 Broker 上什么都不创建，队列不存在就是一次订阅错误：基础
设施由你自己管理。队列归自己所有的服务，为自己的 Broker 开启声明：

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_topology.rs:app"
```

开启声明之后，订阅会依次声明所绑定的交换机（内置的 `amq.*` 和默认交换机除外）、队列和绑定。
只要描述符和已有的一致，声明就是幂等的；属性不同的重复声明会被 Broker 拒绝
（`PRECONDITION_FAILED`）。

## 队列类型 { #queue-types }

RabbitMQ 在声明时通过 `x-queue-type` 选定队列的实现，而已存在的队列的类型再也改不了。描述符把
它作为一个类型化的设置给出：

- `.queue_type(QueueType::Classic)` - 经典的单节点实现。
- `.queue_type(QueueType::Quorum)` - 用 Raft 复制；必须保持持久化。crate 会拒绝声明非持久化的
  仲裁队列，而不是把声明失败留给 Broker。

Broker 级的 `.default_queue_type(..)` 作用于没有自选类型的描述符；两处都没设时，不发送
`x-queue-type`，服务端的默认值生效。

RabbitMQ 4 默认拒绝瞬态（非持久化）的非独占队列：除非队列写了 `.exclusive(true)`，否则保持默认
的持久化。

## 预取 { #prefetch }

Broker 上的 `.prefetch(n)` 为每个订阅设定 `basic.qos` 窗口：同时最多 `n` 条未确认的投递在处理。
背压就是这样传到 Broker 的，因为消费得慢就会让推送变慢，而不是让消息无上限地堆在缓冲里。描述符
用同名的 `.prefetch(n)` 按队列覆盖它。两处都不设时，服务端不加限制。

计数是 `NonZeroU16`。AMQP 把 `basic.qos(0)` 读作「不限」而不是上限为零，所以不设预取才是要求
无限。字面量用框架的 `nonzero!` 宏写，它在编译期拒绝零，上面的描述符就是这么写的。

## 批次 { #batches }

接收切片的处理器消费整个批次，而批次最大多少，由挂载点写出：

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_topology.rs:batches"
```

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_topology.rs:batches_mount"
```

AMQP 没有协议级的批次（Broker 一次只推一个 `basic.deliver`），所以批次在客户端攒出来：投递不断
积累，直到批次凑够挂载点要的大小，或者距第一条投递已过 `.batch_wait(..)`，以先到者为准。挂载点
对此一个字也不说：一次批量注册只写出大小，批次怎么攒属于描述符。

`.batch_wait(..)` 默认 50 毫秒。链路慢或者队列稀疏、攒满一批值得等的时候调大它；一批来晚了比
一批来少了代价更高的时候调小它。

一个批次可以短于该大小，因为不满的批次会直接交给处理器，而不是去等可能永远不来的流量。而且
批次里只有 Broker 已经推过来的消息，所以[预取](#prefetch)窗口窄于批次大小时，每一批都被这个窗口
卡住：`.batch(n)` 要配上不小于 `n` 的预取。

## 投递元数据 { #delivery-metadata }

`AmqpContext` 给出既不属于负载也不属于消息头的 AMQP 投递元数据：交换机、路由键、重新投递标志，
以及信道内的投递标签。每个字段在 `context::keys` 里都有一个零大小的键，处理器用提取器参数点名
自己需要的字段，一个字段一个键：

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_keyed_lanes.rs:metadata"
```

声明了 `ctx: &mut Context<'_, AmqpContext>` 的处理器，用 `ctx.context(KEY)` 读同样的字段。
prelude 导出这些键，但不导出上下文类型，后者从 `ruststream_lapin::context` 导入。

## 按键的工作分区 { #keyed-worker-lanes }

订阅可以用 `workers(n, by_key)` 把处理分到多个工作分区上，并让共享同一个键的投递留在同一个分区里
（键内有序，跨键并行）：

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_keyed_lanes.rs:consumer"
```

生产方把键写进 `PARTITION_KEY_HEADER`（`amqp-partition-key`），crate 通过 `Partitioned` 能力把它
读回来。AMQP 本身不解释该消息头，所以这是一个客户端约定，与下面服务端的哈希路由无关。

## 死信 { #dead-letter }

`.dead_letter_exchange("dlx")`（需要时再加 `.dead_letter_routing_key(..)`）设定队列原生的死信
目标。丢弃消息的处理器按 `basic.reject(requeue = false)` 结算，消息就被路由到那里。

## 延迟重试 { #delayed-retry }

返回 `HandlerOutcome::retry_after(delay)` 的处理器，要求不早于 `delay` 再重新投递，也就是「还没
就绪」这一种情形，此时立刻重新入队只会空转。默认情况下运行时用它与 Broker 无关的兜底路径处理
这件事：延迟的副本在服务进程里等待，整个窗口内是至多一次；它按队列自己的名字发布回去，因为在
默认交换机上正是它寻址到该队列。`.delay(..)` 改用原生方式：消息停在 Broker 的等待队列里，
带着自己的 TTL，TTL 到期就以死信的方式回到原队列，于是延迟的副本存在 Broker 上，窗口中途重启也
不丢东西。

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_topology.rs:delay"
```

等待队列（默认 `<queue>.retry`，或者 `Delay::dlx_ttl_named(..)`）是基础设施：它只在
`declare_topology(true)` 之下才声明，否则你自己去准备。经典队列只从队头释放过期的消息，因此延迟
差别很大时，一个延迟档位用一个等待队列（或者改用仲裁队列）。

延迟高度混杂的负载，可以用 `plugin-dme` 特性提供的 `Delay::plugin_dme()`，它改为让重新投递走
[delayed-message-exchange](https://github.com/rabbitmq/rabbitmq-delayed-message-exchange) 插件：
消息带上 `x-delay` 消息头，插件各自独立地持有每一条，于是短延迟绝不会排在长延迟后面等待。它需要
Broker 上启用该插件，特性开关就是为此而设。

## 一致性哈希交换机（插件） { #consistent-hash-exchange-plugin }

要在服务端分流，也就是按路由键做哈希、把一条流摊到多个队列上，`plugin-consistent-hash` 特性给出
`RabbitExchange::consistent_hash(..)`，它下沉到
[`rabbitmq_consistent_hash_exchange`](https://github.com/rabbitmq/rabbitmq-server/tree/main/deps/rabbitmq_consistent_hash_exchange)
插件的交换机类型。每个队列以自己的整数权重作为路由键来绑定，Broker 按比例切分哈希空间：

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_consistent_hash.rs:shards"
```

一致性哈希路由发生在 Broker 上，分的是队列；而工作分区分的是一个消费者内部的工作。用之前先在
Broker 上启用插件；该特性默认关闭，因为它不在原版 RabbitMQ 里。

## 原始参数 { #raw-arguments }

描述符没有建模的东西，原样发送：

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_topology.rs:arguments"
```

`AMQPValue` 和 `FieldTable` 正是为此从 crate 里重新导出的。
