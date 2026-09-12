# 发布

`LapinPublish` 是构造本 Broker 发布者 `LapinPublisher` 的策略。设置写在策略上，也就是点名 Broker
的地方，运行时在启动时于已连接的 Broker 上实例化发布者。

消息的名字就是路由键。交换机是策略的属性：不用 `.exchange("events")` 点名别的，就走默认交换机。
在默认交换机上，路由键寻址的是同名队列，快速上手因此完全不需要拓扑。

有四个消息头映射到 AMQP 的原生属性：`content-type`、`correlation-id`、`reply-to` 和
`message-id`。其余每个消息头都以字节串进入 AMQP 消息头表，所以二进制值可以原样往返。

## 单条消息的 AMQP 属性 { #per-message-amqp-properties }

有三项 AMQP 属性属于消息而不属于发布者：优先级 `priority`、单条消息的过期时间 `expiration`
（TTL），以及投递模式。挂载点为所有消息定下取值，发布构建器为单条消息改其中一项。

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_priority.rs:mount"
```

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_priority.rs:steps"
```

三个步骤是 `priority(n)`、`expiration(ttl)` 和 `persistent(flag)`，策略用同样这三个名字给出默认
值。一次发布没动过的，就仍然是挂载点定下的。

- `priority` 在以 `x-max-priority` 声明的队列上排定投递顺序；在其他任何队列上，它照样到达
  消费者，但不影响顺序。默认不设。
- `expiration` 让 Broker 在 TTL 走完而消息仍未被消费时删除它，队列这么配了就送进死信。默认不
  设，所以消息不会过期。
- `persistent` 是投递模式 2，也是默认值。Broker 重启丢消息可以接受的地方，才关掉它。

步骤是发布构建器上的一个位置，不是套在发布者外面的包装。因此带步骤的发布保留挂载点给它的编解码
器和目的地，在两种事务里都成立，在 `TestApp` 之下也仍然归属于它的 `Out` 槽位。写步骤的处理器
主体，是主体唯一点名本 crate 的地方：把槽位约束成
`Out<impl Publisher<Options = LapinPublishOptions>, Marker>`，并导入
`ruststream_lapin::prelude::*`。

这三项都不走消息头传递。写进消息头表的值，无论用协议自己的名字（`priority`、`expiration`）还是
用投递报告它们时的名字，到了 RabbitMQ 都只是一条它不作任何用途的表项，消息投递出去时不带这项
属性。

投递确实会把它们作为消息头报告回来，名字是 `amqp-priority` 和 `amqp-expiration`
（`PRIORITY_HEADER` / `EXPIRATION_HEADER`），所以处理器读入站优先级的地方，和它读其他一切的地方
是同一处。

## 从处理器回复 { #replying-from-a-handler }

会发布的处理器返回回复值，由运行时把它发布出去。回复类型用 `#[outgoing(name = "..")]` 声明它发往
哪里。什么都不声明的类型，按 `publish("..")` 子句给的名字发布。

两种名字都是路由键，交换机由挂载点的策略点名：`.out(Reply, Publish::default())` 在默认交换机上
回复，`.out(Reply, Publish::default().exchange("events"))` 在主题交换机上回复。其后的步骤把回复
接线的其余部分补齐：`.codec(..)` 指定回复的编解码器，`.transform(..)` 在发布前改动每一条回复。

完整的回复表面写在[核心的发布指南](https://powersemmi.github.io/ruststream/)里。
[请求与回复页面](request-reply.md)给出 RPC 变体，那里由一个变换把每条回复重定向到请求方的私有
地址。

## 三个发布者 { #three-publishers }

`LapinPublish::default()` 发完就不再等待：帧写出去，这次发布就完成，没有来自 Broker 的反馈。发布
模式是一次策略跃迁，所以更强的保证是另一个类型：

- `.confirms()` - `ConfirmsPublish`，发布者确认：Broker 确认之后，这次发布才完成。事务在客户端
  缓冲，提交时刷出。持久又快；推荐的事务发布者。
- `.server_tx()` - `ServerTxPublish`，AMQP 信道事务（`tx.select` / `tx.commit` /
  `tx.rollback`）：消息在提交时原子地变为可见。代价是每次提交一趟同步往返；不能接受部分刷出
  时，它是唯一选择。

路由文件按整个家族统一的挂载点名字写出它用到的那两个，别名由 [prelude](index.md) 给出：`Publish`
是 `LapinPublish`，`TransactionalPublish` 是 `ConfirmsPublish`，于是路由器无论针对哪个 Broker
写，读起来都一样。`ServerTxPublish` 保留自己的名字：它是另一种保证，不是同一件事的第二种写法。

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_transactions.rs:confirms"
```

取舍在于：确认给出逐条消息的持久性，一次提交失败时，先前的消息可能已经发布出去了；服务端事务
给出全有或全无的可见性。

## 从处理器做事务分发 { #transactional-fan-out-from-a-handler }

在挂载点把策略绑定到处理器的槽位（`.out(marker, policy)`，以 `.build()` 收尾），处理器就以 `Out`
参数拿到活的发布者。下面的例子里，一个订单展开成按条目的发货指令，全有或全无地发布出去：

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_transactions.rs:dispatch"
```

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_transactions.rs:handler"
```

两个事务发布者都实现框架的 `TransactionalPublisher`，所以任何一个都能接进同一组
`begin_transaction / commit / abort` 调用点。在当前状态下非法的调用会返回错误，而不是默默放过：
没有打开事务就提交或中止，以及已经打开时再次 `begin`，后者会让已打开的事务保持原样。发布者是
引用计数的句柄，所以它的每个副本共用同一个信道和同一份事务状态。

## 拥有式事务与借用式事务 { #owned-or-borrowed-transactions }

框架有两种事务形态，发布者提供哪些取决于传输：

- **借用式** - 事务由句柄持有。`begin()` 给出一个作用域，或者直接在发布者上调用
  `begin_transaction / commit / abort`。一个句柄上只能打开一个，所以第二次 `begin` 返回错误。
  两个发布者都支持它。
- **拥有式** - 事务是一个拥有自己缓冲的值，由 `owned_transaction()` 打开（在发布者上则是
  `OwnedTransactions::transaction`）。
  一个句柄上同时开多少个都行，结算其中一个绝不影响另一个，其间句柄本身照常直接发布。`commit`
  和 `abort` 消耗掉该值，所以重复提交或者结算后再发布是编译错误。只有 confirms 发布者支持
  它：它的事务是一个客户端缓冲，而 `server_tx` 把信道本身切进事务模式，那是信道状态，只存在
  一份。

一个处理器要驱动几组互不相干的消息时，用拥有式；一整段作用域的代码往同一个共享事务里发布时，用
借用式。提交失败时，拥有式事务已被消耗，它的缓冲也随之丢失：恢复路径是让输入消息重新投递，而
不是重新提交该缓冲。
