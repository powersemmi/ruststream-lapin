//! Terminal recovery and socket cleanup through the public broker and subscriber APIs.

use std::process::id;
use std::time::Duration;

use futures::StreamExt;
use ruststream::{Broker, ConnectedBroker, OutgoingMessage, Publisher, Subscriber};
use ruststream_lapin::{AmqpError, LapinBroker, LapinPublish, RabbitQueue};
use tokio::net::TcpListener;
use tokio::time::timeout as timeout_future;

use crate::{live, proxy};

#[tokio::test]
async fn exhausted_reconnects_end_the_consumer_instead_of_hanging() {
    let Some(url) = live::url("AMQP_TEST_URL") else {
        return;
    };
    let proxy = proxy::Proxy::new(&url).await;
    let broker = LapinBroker::new(&proxy.url)
        .auto_recover(true)
        .recovery_retries(1)
        .declare_topology(true)
        .connect()
        .await
        .expect("connect");
    let queue = format!("ruststream.exhausted-recovery.{}", id());
    let mut consumer = broker
        .subscribe(RabbitQueue::new(&queue).durable(false).exclusive(true))
        .await
        .expect("consumer");
    drop(proxy);
    let mut stream = consumer.stream().boxed();
    let result = timeout_future(Duration::from_secs(10), stream.next())
        .await
        .expect("bounded recovery must finish")
        .expect("terminal failure is reported");
    assert!(result.is_err());
    assert!(stream.next().await.is_none());
    assert!(!broker.is_connected());
    drop(stream);
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn exhausted_handshake_retries_notify_the_consumer() {
    let Some(url) = live::url("AMQP_TEST_URL") else {
        return;
    };
    let proxy = proxy::Proxy::new(&url).await;
    let broker = LapinBroker::new(&proxy.url)
        .auto_recover(true)
        .recovery_retries(1)
        .declare_topology(true)
        .connect()
        .await
        .expect("connect");
    let queue = format!("ruststream.exhausted-handshake.{}", id());
    let mut consumer = broker
        .subscribe(RabbitQueue::new(&queue).durable(false).exclusive(true))
        .await
        .expect("consumer");
    // TCP connects, but the proxy closes before the AMQP handshake can finish.
    proxy.offline();
    let mut stream = consumer.stream().boxed();
    let result = timeout_future(Duration::from_secs(10), stream.next())
        .await
        .expect("bounded recovery must finish")
        .expect("terminal failure is reported");
    assert!(result.is_err());
    assert!(stream.next().await.is_none());
    assert!(!broker.is_connected());
    drop(stream);
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn zero_retries_fails_the_initial_connection_without_retrying() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("reserve port");
    let address = listener.local_addr().expect("local address");
    drop(listener);
    let result = timeout_future(
        Duration::from_secs(5),
        LapinBroker::new(format!("amqp://{address}"))
            .auto_recover(true)
            .recovery_retries(0)
            .connect(),
    )
    .await
    .expect("zero retries must fail promptly");
    assert!(matches!(result, Err(AmqpError::Connect(_))));
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
