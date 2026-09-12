# 测试

不用 RabbitMQ 服务器也能测 RabbitMQ 服务。`testing` 特性提供 `LapinTestBroker`，一个进程内替身，
跑的是和生产一样的处理器、描述符和接线。它按队列名精确路由，走默认交换机模型，并记录每一次发布，
所以测试可以对处理器发出的内容做断言。

围着它组装应用，和你组装真实应用的写法完全一样，再把它交给框架的 `TestApp` 测试套件。每一次经过
测试套件的发布，都把处理器推进到静止之后才返回，所以其后的断言绝不会和它们发生竞态。

```toml
[dev-dependencies]
ruststream-lapin = { version = "0.7", features = ["testing"] }
```

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_testing.rs:handler"
```

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_testing.rs:testapp"
```

## 路由文件不变 { #the-routes-file-does-not-change }

挂载点通常是测试开始偏离生产的地方，这里却不偏离：crate 的发布策略对着测试 Broker 同样构造得出
发布者。每一个都给出它在服务器上所产出发布者的替身，带着同样的能力：

| 挂载点 | 在 RabbitMQ 上 | 在 `LapinTestBroker` 上 | 能力 |
| --- | --- | --- | --- |
| `Publish` | `LapinPublisher` | `LapinTestPublisher` | 发布 |
| `TransactionalPublish` | `ConfirmsPublisher` | `ConfirmsTestPublisher` | 发布、借用式和拥有式事务 |
| `ServerTxPublish` | `ServerTxPublisher` | `ServerTxTestPublisher` | 发布、借用式事务 |
| `Request` | `LapinRequester` | `LapinTestRequester` | 发布、请求与回复 |

这种对等是双向的，把它写出来正是为了这一点。约束成 `Out<impl TransactionalPublisher>` 的处理器
挂到 `Publish` 上，对着测试 Broker 编译不过，对着服务器也一样编译不过，于是通过的测试绝不会覆盖
上不了线的接线。

请求与回复也一并如此。在流程中途调用另一个服务的处理器（`Out<impl RequestReply>`，用
`Request::default()` 挂载），在进程内对着同一个应用里的应答方运行：传输给每个请求一个私有的回复
地址和一个 correlation id，`DirectReplyTo` 变换把回复送回那里，而对不上号的回复会被丢弃，不会让
这次调用完成。无人应答的请求返回和服务器一样的超时错误，处理器实际编写的正是这个分支。

`&[T]` 处理器原封不动地挂到测试 Broker 上：传输在客户端攒[批次](queues.md#batches)，和真实订阅者
的做法完全一致，而 `assert_batch_sizes(..)` 报告主体收到的那些批次。

## 对单条消息的属性做断言 { #asserting-on-per-message-properties }

为单条消息调整某项 [AMQP 属性](publishing.md#per-message-amqp-properties)的处理器，从两侧做断言。
`with_options(..)` 读回发布构建器的步骤要求了什么，`assert_options_default()` 断言没有任何步骤
执行过、生效的是挂载点自己的设置。两者都在槽位视图 `tb.out::<Marker>()` 上。

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_testing.rs:options"
```

消费者最终看到的是解析后的取值，传输报告它的方式和真实投递一样，也就是 `amqp-priority` 和
`amqp-expiration` 两个消息头。所以无论取值来自步骤还是来自挂载点，Broker 的发布日志断言的都是
结果：

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_testing.rs:delivered"
```

通过 [`AmqpContext`](queues.md#delivery-metadata) 读 AMQP 字段的处理器，也原封不动地挂上去。传输
按自己的模型填这些字段：默认交换机、作为路由键的队列名、按订阅编号的投递标签，以及重新入队时置
上的 `redelivered`。

有多个消费者的队列本来就是工作队列，行为也正是如此：每一次投递恰好交给其中一个，轮流分配，而
`nack(requeue = true)` 重新走一遍队列，不会回到拒绝它的那个消费者。处理器的横向扩展因此可以在
进程内测试：同一个队列上的两个订阅分摊工作，而不是各拿一份副本。

## 测试 Broker 不模拟什么 { #what-the-test-broker-does-not-simulate }

该传输建模的是路由和消费者，不是存储：队列只作为监听该名字的那组订阅存在，消息存在消费者的
信道里。下面每一条都由这一句推出来，而且每一条都改由 crate 的集成测试对着真实服务器检验。

- **没有东西等消费者。** 发布到无人消费的队列的消息会被丢弃，和无法路由的消息一样；而一个订阅
  只看得到它打开之后才发布的消息。
- **预取。** 没有积压可存放消息，也就没有什么可扣住的：投递一到就推出去，没有消费者会因为到了
  未确认上限而被跳过，`prefetch(..)` 也不改变分配。预取的上限是服务器的行为。
- **死信。** 被丢弃的消息到此为止。死信交换机要通过绑定表来解析，而那正是该传输不建模的另一
  半。
- **交换机与绑定。** 路由按队列名精确匹配，所以策略的 `exchange(..)` 被接受并忽略。交换机类型、
  绑定和备用交换机那条路径都需要服务器。
- **发布者确认。** `TransactionalPublish` 一丝不差地重现客户端缓冲：缓冲、提交时按序重放、中止时
  丢弃；但进程内没有任何东西能 nack 一条消息，所以提交成功并不能证明 Broker 收下了这一批。
- **AMQP 服务端事务。** `ServerTxPublish` 靠把消息扣在客户端而不是 Broker 上来重现可见性边界，
  所以测试能证明它们何时变为可见，却证明不了原子性，也证明不了连接丢失会让在途的事务怎样。
- **direct reply-to 至多一次的性质。** 这里的回复地址是订阅，而不是单个 Broker 节点上的信道状
  态，所以回复不会随连接一起丢失。

这些都拿真实的 RabbitMQ 去检验。crate 自己的集成测试就是这么跑的，由 `AMQP_TEST_URL` 开启：

```text
just brokers-up
AMQP_TEST_URL=amqp://127.0.0.1:5672 cargo test --workspace --all-features -- --test-threads=1
```
