`RabbitMQ` over AMQP 0.9.1 for [RustStream](https://github.com/powersemmi/ruststream)
services, backed by the [`lapin`] client.

A subscription consumes one queue, and a publish is one message on an exchange under a routing
key. A queue is destructive storage: an acknowledged delivery leaves it. Acknowledgements,
requeues and dead-letter routes are protocol frames, so the framework settles a message on the
server and keeps no copy in the service.

# A service

A handler is an `async fn` over the decoded payload, mounted on a broker inside the
application object, and the attribute writes the `main`:

```
# mod demo {
use ruststream_lapin::prelude::*;
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u64,
}

#[subscriber("orders")]
async fn handle(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "accepted");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        LapinBroker::new("amqp://localhost:5672").prefetch(nonzero!(64)),
        |b| {
            b.include(handle);
        },
    )
}
# }
# fn main() {}
```

`cargo run -- run` starts it. [`LapinBroker::new`] only records the configuration; the runtime
connects the broker at startup and shuts it down when the service stops. The queue `orders` has
to exist, or the broker declares it under
[`declare_topology(true)`](LapinBroker::declare_topology).

# Where things are

The [guides][guides] take a `RabbitMQ` service from installation to production, in this order:
[getting started][g-start], [work queues][g-work-queues], [exchanges and routing][g-routing],
[topology][g-topology], [acknowledgements, retries and dead letters][g-retries],
[reliable publishing][g-publishing], [request and reply][g-rpc], [testing][g-testing] and
[running in production][g-production]. The [architecture][architecture] pages show how the crate
sits between `lapin` and the runtime, and the [benchmarks][benchmarks] what it costs.

Each topic below links to its component page on the site, which shows it in a service; the
item's own documentation keeps the detail.

## Broker

- [LapinBroker][c-broker]: [`LapinBroker`], connected into [`ConnectedLapinBroker`] and closed
  into [`ClosedLapinBroker`].
- [Connection][c-connection]: the URI, [`LapinBroker::connection_name`], opt-in
  [`LapinBroker::auto_recover`], [`LapinBroker::recovery_retries`] and
  [`ConnectedLapinBroker::is_connected`].
- [TLS][c-tls]: `amqps://` URIs with the `tls-native-tls`, `tls-rustls` or `tls-rustls-ring`
  feature.
- [declare_topology][c-declare-topology]: [`LapinBroker::declare_topology`] declares what the
  descriptors state.
- [Shutdown][c-shutdown]: handles that outlive the connection report [`AmqpError::Closed`].
- [Prelude][c-prelude]: [`prelude`], the one glob a `RabbitMQ` service imports.
- [Errors][c-errors]: [`AmqpError`].

## Subscriptions

- [Subscriptions][c-subscriptions]: a bare queue name, consumed as a classic queue.
- [RabbitQueue][c-rabbit-queue]: [`RabbitQueue`], a classic queue and the settings both
  descriptors share.
- [RabbitQuorumQueue][c-rabbit-quorum-queue]: [`RabbitQuorumQueue`], a replicated queue that
  counts deliveries.
- [prefetch][c-prefetch]: [`LapinBroker::prefetch`] and [`RabbitQueue::prefetch`] cap the
  deliveries in flight.
- [Arguments][c-arguments]: [`RabbitQueue::argument`] passes any `x-` argument, with
  [`AMQPValue`] and [`FieldTable`].
- [Batches][c-batches]: a slice handler over [`LapinSubscriber`] batches and
  [`RabbitQueue::batch_wait`].
- [Lanes][c-lanes]: `workers(n, by_key)` keyed by [`PARTITION_KEY_HEADER`].

## Exchanges

- [RabbitExchange][c-exchanges]: [`RabbitExchange`] and [`RabbitQueue::bind`], with the direct
  exchange and a plugin's own type through [`RabbitExchange::custom`].
- [topic][c-topic]: [`RabbitExchange::topic`], routing on dotted patterns.
- [fanout][c-fanout]: [`RabbitExchange::fanout`], a copy to every bound queue.
- [bind_with][c-bind-with]: [`RabbitQueue::bind_with`] and [`RabbitExchange::headers`], routing on
  headers.
- [consistent_hash][c-consistent-hash]: `RabbitExchange::consistent_hash`, the consistent-hash
  plugin exchange (feature `plugin-consistent-hash`).

## Settlement

- [Settlement][c-settlement]: what ack, retry and drop send, on [`LapinMessage`].
- [dead_letter_exchange][c-dead-letter-exchange]: [`RabbitQueue::dead_letter_exchange`] and
  [`RabbitQueue::dead_letter_routing_key`].
- [Retries][c-retries]: `max_attempts` and `dead_letter` as queue arguments: both on a quorum
  queue, `dead_letter` alone on a classic queue.
- [delivery_limit][c-delivery-limit]: [`RabbitQuorumQueue::delivery_limit`], the quorum queue's
  own cap.
- [retry_after][c-retry-after]: a delayed redelivery through the queue's waiting queue.
- [delay][c-delay]: [`RabbitQueue::delay`] with [`Delay::dlx_ttl`], a waiting queue on the
  server.
- [plugin_dme][c-plugin-dme]: `Delay::plugin_dme`, the delayed-message exchange (feature
  `plugin-dme`).

## Context

- [AmqpContext][c-context]: [`AmqpContext`](context::AmqpContext) and the keys in
  [`context::keys`]: exchange, routing key, redelivered flag and delivery tag.

## Publishing

- [LapinPublish][c-publishing]: [`LapinPublish`], the plain publish policy, and its live
  [`LapinPublisher`].
- [exchange][c-exchange]: [`LapinPublish::exchange`], the exchange a policy publishes to.
- [Headers][c-headers]: the headers that ride native AMQP properties.
- [Properties][c-properties]: [`LapinPublishSteps`] and [`LapinPublishOptions`] set one
  message's properties.
- [priority][c-priority], [expiration][c-expiration] and [persistent][c-persistent]: the three
  properties, read back through [`PRIORITY_HEADER`] and [`EXPIRATION_HEADER`].

## Transactions

- [Transactions][c-transactions]: the borrowed transaction on both transactional policies.
- [ConfirmsPublish][c-confirms-publish]: [`ConfirmsPublish`] and its live
  [`ConfirmsPublisher`].
- [ServerTxPublish][c-server-tx-publish]: [`ServerTxPublish`] and its live
  [`ServerTxPublisher`].
- [ConfirmsTransaction][c-confirms-transaction]: [`ConfirmsTransaction`], an owned transaction.

## Request and reply

- [LapinRequest][c-request]: [`LapinRequest`] and its live [`LapinRequester`] over direct
  reply-to.
- [DirectReplyTo][c-direct-reply-to]: [`DirectReplyTo`], the responder's reply transform.

## Tooling

- [AsyncAPI][c-asyncapi]: the AMQP bindings of the generated document (feature `asyncapi`):
  [server][c-asyncapi-server], [channels][c-asyncapi-channels],
  [operations][c-asyncapi-operations] and the [extension][c-asyncapi-extension] for bindings.
- [Testing][c-testing]: the in-process transport
  [`TestApp`](https://docs.rs/ruststream/latest/ruststream/testing/struct.TestApp.html) connects
  the broker to (feature `testing`), and [Live][c-live] runs against a running server.

# Cargo features

`asyncapi` contributes the AMQP bindings to the generated document, and `testing` adds the
in-process mode `TestApp::start` connects the broker through. `tls-native-tls`, `tls-rustls` and
`tls-rustls-ring` map onto `lapin`'s own TLS backends. `plugin-consistent-hash` and `plugin-dme`
add the typed forms of two server plugins, which must be enabled on the server.

[`lapin`]: https://docs.rs/lapin
[guides]: https://powersemmi.github.io/ruststream-lapin/latest/
[g-start]: https://powersemmi.github.io/ruststream-lapin/latest/guides/getting-started/
[g-work-queues]: https://powersemmi.github.io/ruststream-lapin/latest/guides/work-queues/
[g-routing]: https://powersemmi.github.io/ruststream-lapin/latest/guides/routing/
[g-topology]: https://powersemmi.github.io/ruststream-lapin/latest/guides/topology/
[g-retries]: https://powersemmi.github.io/ruststream-lapin/latest/guides/retries/
[g-publishing]: https://powersemmi.github.io/ruststream-lapin/latest/guides/publishing/
[g-rpc]: https://powersemmi.github.io/ruststream-lapin/latest/guides/rpc/
[g-testing]: https://powersemmi.github.io/ruststream-lapin/latest/guides/testing/
[g-production]: https://powersemmi.github.io/ruststream-lapin/latest/guides/production/
[architecture]: https://powersemmi.github.io/ruststream-lapin/latest/architecture/
[benchmarks]: https://powersemmi.github.io/ruststream-lapin/latest/architecture/benchmarks/
[c-broker]: https://powersemmi.github.io/ruststream-lapin/latest/components/broker/
[c-connection]: https://powersemmi.github.io/ruststream-lapin/latest/components/broker/connection/
[c-tls]: https://powersemmi.github.io/ruststream-lapin/latest/components/broker/tls/
[c-declare-topology]: https://powersemmi.github.io/ruststream-lapin/latest/components/broker/declare-topology/
[c-shutdown]: https://powersemmi.github.io/ruststream-lapin/latest/components/broker/shutdown/
[c-prelude]: https://powersemmi.github.io/ruststream-lapin/latest/components/broker/prelude/
[c-errors]: https://powersemmi.github.io/ruststream-lapin/latest/components/broker/errors/
[c-subscriptions]: https://powersemmi.github.io/ruststream-lapin/latest/components/subscriptions/
[c-rabbit-queue]: https://powersemmi.github.io/ruststream-lapin/latest/components/subscriptions/rabbit-queue/
[c-rabbit-quorum-queue]: https://powersemmi.github.io/ruststream-lapin/latest/components/subscriptions/rabbit-quorum-queue/
[c-prefetch]: https://powersemmi.github.io/ruststream-lapin/latest/components/subscriptions/prefetch/
[c-arguments]: https://powersemmi.github.io/ruststream-lapin/latest/components/subscriptions/arguments/
[c-batches]: https://powersemmi.github.io/ruststream-lapin/latest/components/subscriptions/batches/
[c-lanes]: https://powersemmi.github.io/ruststream-lapin/latest/components/subscriptions/lanes/
[c-exchanges]: https://powersemmi.github.io/ruststream-lapin/latest/components/exchanges/
[c-topic]: https://powersemmi.github.io/ruststream-lapin/latest/components/exchanges/topic/
[c-fanout]: https://powersemmi.github.io/ruststream-lapin/latest/components/exchanges/fanout/
[c-bind-with]: https://powersemmi.github.io/ruststream-lapin/latest/components/exchanges/bind-with/
[c-consistent-hash]: https://powersemmi.github.io/ruststream-lapin/latest/components/exchanges/consistent-hash/
[c-settlement]: https://powersemmi.github.io/ruststream-lapin/latest/components/settlement/
[c-dead-letter-exchange]: https://powersemmi.github.io/ruststream-lapin/latest/components/settlement/dead-letter-exchange/
[c-retries]: https://powersemmi.github.io/ruststream-lapin/latest/components/settlement/retries/
[c-delivery-limit]: https://powersemmi.github.io/ruststream-lapin/latest/components/settlement/delivery-limit/
[c-retry-after]: https://powersemmi.github.io/ruststream-lapin/latest/components/settlement/retry-after/
[c-delay]: https://powersemmi.github.io/ruststream-lapin/latest/components/settlement/delay/
[c-plugin-dme]: https://powersemmi.github.io/ruststream-lapin/latest/components/settlement/plugin-dme/
[c-context]: https://powersemmi.github.io/ruststream-lapin/latest/components/context/
[c-publishing]: https://powersemmi.github.io/ruststream-lapin/latest/components/publishing/
[c-exchange]: https://powersemmi.github.io/ruststream-lapin/latest/components/publishing/exchange/
[c-headers]: https://powersemmi.github.io/ruststream-lapin/latest/components/publishing/headers/
[c-properties]: https://powersemmi.github.io/ruststream-lapin/latest/components/publishing/properties/
[c-priority]: https://powersemmi.github.io/ruststream-lapin/latest/components/publishing/properties/priority/
[c-expiration]: https://powersemmi.github.io/ruststream-lapin/latest/components/publishing/properties/expiration/
[c-persistent]: https://powersemmi.github.io/ruststream-lapin/latest/components/publishing/properties/persistent/
[c-transactions]: https://powersemmi.github.io/ruststream-lapin/latest/components/transactions/
[c-confirms-publish]: https://powersemmi.github.io/ruststream-lapin/latest/components/transactions/confirms-publish/
[c-server-tx-publish]: https://powersemmi.github.io/ruststream-lapin/latest/components/transactions/server-tx-publish/
[c-confirms-transaction]: https://powersemmi.github.io/ruststream-lapin/latest/components/transactions/confirms-transaction/
[c-request]: https://powersemmi.github.io/ruststream-lapin/latest/components/request/
[c-direct-reply-to]: https://powersemmi.github.io/ruststream-lapin/latest/components/request/direct-reply-to/
[c-asyncapi]: https://powersemmi.github.io/ruststream-lapin/latest/components/asyncapi/
[c-asyncapi-server]: https://powersemmi.github.io/ruststream-lapin/latest/components/asyncapi/server/
[c-asyncapi-channels]: https://powersemmi.github.io/ruststream-lapin/latest/components/asyncapi/channels/
[c-asyncapi-operations]: https://powersemmi.github.io/ruststream-lapin/latest/components/asyncapi/operations/
[c-asyncapi-extension]: https://powersemmi.github.io/ruststream-lapin/latest/components/asyncapi/extension/
[c-testing]: https://powersemmi.github.io/ruststream-lapin/latest/components/testing/
[c-live]: https://powersemmi.github.io/ruststream-lapin/latest/components/testing/live/
