# 请求与回复

基于 RabbitMQ [direct reply-to](https://www.rabbitmq.com/docs/direct-reply-to) 的 RPC，是一个服务
通过手边已有的 Broker 去问另一个服务，而不必再长出一条 HTTP 旁路：订单服务在接单之前，先到库存
服务查一下有没有货。两半都是普通的服务；跑得起来的一对是
[`lapin_rpc_server`](https://github.com/powersemmi/ruststream-lapin/blob/main/crates/ruststream-lapin/examples/lapin_rpc_server.rs)
和 [`lapin_rpc_client`](https://github.com/powersemmi/ruststream-lapin/blob/main/crates/ruststream-lapin/examples/lapin_rpc_client.rs)。

## 请求方 { #the-requester }

`LapinRequest` 是构造请求方 `LapinRequester` 的策略，而后者实现 `RequestReply` 能力。每个请求发出
时，`reply-to` 指向 direct reply-to 伪队列，并带上生成的 `correlation-id`，对应的回复让这次调用
完成。把这项能力包进一个小的类型化调用：

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_rpc_client.rs:client"
```

在挂载点把策略绑定到处理器的槽位
（`b.include(handler).out(DefaultSlot, Request::default()).build()`），
处理器就以 `Out` 参数拿到活的请求方，于是它在自己的消息流程中途去调另一个服务。失败的边界是 RPC
超时：业务上的答复结算这条消息，服务不可达则请求重新投递：

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_rpc_client.rs:handler"
```

## 应答方 { #the-responder }

应答方就是一个普通的会发布的处理器：解码请求，返回回复。`Err` 不回复就结算，恢复机制是请求方
那边的超时。

RPC 回复发往请求要求的地方，所以它的类型不能声明自己的目的地：`DirectReplyTo` 为每次投递点名
目的地，而挂载点会拒绝把它用在已经声明过目的地的回复类型上。`publish("..")` 子句为不带 reply-to
消息头到达的请求点名地址，生成的 AsyncAPI 文档报告的也是它：

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_rpc_server.rs:handler"
```

让它成为 RPC 应答方的，是那个变换。`DirectReplyTo` 把每条回复送回请求方要求的地址，并回传它的
correlation id；在挂载时把它组合到回复发布者上：

```rust
--8<-- "crates/ruststream-lapin/examples/lapin_rpc_server.rs:mount"
```

处理器仍然是一个从请求到回复的纯函数，和其他处理器一样可以在进程内测试。这一对也一样：
`LapinRequest` 对着 [`LapinTestBroker`](testing.md) 同样构造得出请求方，在那里也给每个请求一个
私有的回复地址和一个 correlation id，于是 RPC 客户端处理器和它的应答方，就带着各自随附的挂载点
在 `TestApp` 之下互相对跑。进程内传输替代不了的，是下面这种至多一次的性质。

## 语义 { #semantics }

- **至多一次。** direct reply-to 把回复状态放在请求方的信道里，在单个 Broker 节点上，不做任何
  持久排队。信道断开就丢掉在途的回复，而无人应答的请求返回一个超时错误。
- **默认瞬态。** 请求以投递模式 1 发布，而普通的发布是持久化的。你可以在策略上用
  `.persistent(true)` 把它们标为持久化。
- **不需要基础设施。** 伪队列从不声明，所以这里唯一真实存在的实体，是应答方消费的那个请求队列。
  另一套技术栈上的应答方也能与之互通，只要它把回复发布到收到的 `reply-to` 并回传
  `correlation-id`。
