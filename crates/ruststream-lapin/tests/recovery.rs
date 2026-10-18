//! Terminal recovery and socket cleanup through the public broker and subscriber APIs.

use std::time::{Duration, Instant};

use futures::{FutureExt, StreamExt};
use ruststream::{Broker, ConnectedBroker, OutgoingMessage, Publisher, Subscriber};
use ruststream_lapin::{AmqpError, LapinBroker, LapinPublish, RabbitQueue};

mod live;
#[path = "live/proxy.rs"]
mod proxy;

#[tokio::test]
async fn exhausted_reconnects_end_the_consumer_instead_of_hanging() {
    let Some(url) = live::url("AMQP_TEST_URL") else {
        return;
    };
    let proxy = proxy::Proxy::new(&url).await;
    let broker = LapinBroker::new(&proxy.url)
        .auto_recover(true)
        .declare_topology(true)
        .connect()
        .await
        .expect("connect");
    let queue = format!("ruststream.exhausted-recovery.{}", std::process::id());
    let mut consumer = broker
        .subscribe(RabbitQueue::new(&queue).durable(false).exclusive(true))
        .await
        .expect("consumer");
    drop(proxy);
    let mut stream = consumer.stream().boxed();
    let started = Instant::now();
    tokio::time::pause();
    loop {
        // Advance TCP retry timers; Lapin's IO thread also needs real time to observe socket errors.
        tokio::time::advance(Duration::from_secs(60)).await;
        tokio::task::yield_now().await;
        if let Some(result) = stream.next().now_or_never() {
            assert!(result.expect("terminal failure is reported").is_err());
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "bounded recovery must finish"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    tokio::time::resume();
    assert!(stream.next().await.is_none());
    assert!(!broker.is_connected());
    drop(stream);
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_closes_the_socket_when_it_recovers() {
    let Some(url) = live::url("AMQP_TEST_URL") else {
        return;
    };
    let proxy = proxy::Proxy::new(&url).await;
    let broker = LapinBroker::new(&proxy.url)
        .auto_recover(true)
        .connect()
        .await
        .expect("connect");
    let publisher = broker.publisher(LapinPublish::default());
    let (original, _) = proxy.connections();
    proxy.offline();
    proxy.wait_for(|| !broker.is_connected()).await;
    broker.shutdown().await.expect("shutdown");
    proxy.online();
    proxy
        .wait_for(|| {
            let (accepted, active) = proxy.connections();
            accepted > original && active == 0
        })
        .await;
    assert!(matches!(
        publisher
            .publish(OutgoingMessage::new("queue", b"closed"), None)
            .await,
        Err(AmqpError::Closed { .. })
    ));
}
