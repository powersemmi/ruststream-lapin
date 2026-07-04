//! Integration tests for the plugin-gated features, against a `RabbitMQ` with the plugins
//! enabled.
//!
//! Each test is a no-op unless `AMQP_PLUGINS_TEST_URL` points at such a broker (see
//! `docker/rabbitmq-plugins.dockerfile` and the `plugins` compose profile):
//!
//! ```text
//! just plugins-up
//! AMQP_PLUGINS_TEST_URL=amqp://127.0.0.1:5673 \
//!   cargo test -p ruststream-lapin --features plugin-consistent-hash --test plugins_lapin \
//!   -- --test-threads=1
//! ```

#![cfg(feature = "plugin-consistent-hash")]

use std::time::Duration;

use futures::StreamExt;
use ruststream::{Broker, IncomingMessage, OutgoingMessage, Publisher, Subscriber};
use ruststream_lapin::{LapinBroker, RabbitExchange, RabbitQueue};

fn plugins_url() -> Option<String> {
    std::env::var("AMQP_PLUGINS_TEST_URL").ok()
}

fn unique(base: &str) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("ruststream-plugin.{base}.{}-{n}", std::process::id())
}

/// Drains everything a subscriber delivers within a short quiet window, acking each, and returns
/// the count.
async fn drain(sub: &mut ruststream_lapin::LapinSubscriber) -> u32 {
    let mut stream = Box::pin(sub.stream());
    let mut count = 0;
    while let Ok(Some(Ok(msg))) =
        tokio::time::timeout(Duration::from_millis(300), stream.next()).await
    {
        count += 1;
        msg.ack().await.expect("ack");
    }
    count
}

/// A consistent-hash exchange should split published messages across the queues bound to it, so
/// two equally weighted shards each receive part of the stream.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consistent_hash_exchange_distributes_across_shards() {
    let Some(url) = plugins_url() else { return };
    let broker = LapinBroker::new(url).declare_topology(true);
    Broker::connect(&broker).await.expect("connect");

    let exchange = unique("hash");
    let shard_a = unique("shard-a");
    let shard_b = unique("shard-b");
    // Both shards bind to the hash exchange with weight 1; the binding key is the weight.
    let hash = || {
        RabbitExchange::consistent_hash(&exchange)
            .durable(false)
            .auto_delete(true)
    };
    let mut a = broker
        .subscribe(
            RabbitQueue::new(&shard_a)
                .durable(false)
                .exclusive(true)
                .bind(hash(), "1"),
        )
        .await
        .expect("subscribe shard a");
    let mut b = broker
        .subscribe(
            RabbitQueue::new(&shard_b)
                .durable(false)
                .exclusive(true)
                .bind(hash(), "1"),
        )
        .await
        .expect("subscribe shard b");

    // Publish many distinct routing keys so the hash spreads them across both shards.
    let publisher = broker.publisher().exchange(&exchange);
    let total = 40u32;
    for i in 0..total {
        publisher
            .publish(OutgoingMessage::new(&format!("key-{i}"), &i.to_be_bytes()))
            .await
            .expect("publish");
    }

    // Drain both shards for a short while; count what each received.
    let got_a = drain(&mut a).await;
    let got_b = drain(&mut b).await;

    assert_eq!(
        got_a + got_b,
        total,
        "every message must reach exactly one shard"
    );
    assert!(
        got_a > 0,
        "shard a received nothing: hash did not distribute"
    );
    assert!(
        got_b > 0,
        "shard b received nothing: hash did not distribute"
    );

    broker.shutdown().await.expect("shutdown");
}
