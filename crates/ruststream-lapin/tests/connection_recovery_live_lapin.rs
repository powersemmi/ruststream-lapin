//! Connection outages through a private proxy; never restarts the shared `RabbitMQ` server.

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use futures::StreamExt;
use lapin::types::FieldTable;
use ruststream::{
    BatchSubscriber, Broker, ConnectedBroker, IncomingMessage, OutgoingMessage, Publisher,
    RequestReply, Subscriber,
};
use ruststream_lapin::{
    AmqpError, Delay, LapinBroker, LapinPublish, LapinRequest, RabbitExchange, RabbitQueue,
    RabbitQuorumQueue,
};

mod live;
#[path = "live/proxy.rs"]
mod proxy;

const WAIT: Duration = Duration::from_secs(30);

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn initial_connection_retries_until_the_server_is_available() {
    let Some(url) = live::url("AMQP_TEST_URL") else {
        return;
    };
    let proxy = proxy::Proxy::new(&url).await;
    proxy.offline();
    let connecting = LapinBroker::new(&proxy.url).auto_recover(true).connect();
    tokio::pin!(connecting);
    assert!(
        tokio::time::timeout(Duration::from_millis(150), &mut connecting)
            .await
            .is_err()
    );
    proxy.online();
    let broker = tokio::time::timeout(WAIT, connecting)
        .await
        .expect("startup retry timeout")
        .expect("connect after startup outage");
    assert!(broker.is_connected());
    broker.shutdown().await.expect("shutdown");
}

fn unique() -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    format!(
        "ruststream.connection-recovery.{}.{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

async fn message(
    subscriber: &mut ruststream_lapin::LapinSubscriber,
) -> ruststream_lapin::LapinMessage {
    tokio::time::timeout(WAIT, subscriber.stream().boxed().next())
        .await
        .expect("delivery timeout")
        .expect("stream stays open")
        .expect("delivery")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn existing_handles_resume_after_repeated_outages() {
    let Some(url) = live::url("AMQP_TEST_URL") else {
        return;
    };
    let proxy = proxy::Proxy::new(&url).await;
    let broker = LapinBroker::new(&proxy.url)
        .auto_recover(true)
        .declare_topology(true)
        .prefetch(ruststream::nonzero!(2))
        .connect()
        .await
        .expect("connect");
    let name = unique();
    let mut subscriber = broker
        .subscribe(
            RabbitQueue::new(&name)
                .durable(false)
                .exclusive(true)
                .bind(RabbitExchange::direct(&name).durable(false), "key")
                .delay(Delay::dlx_ttl()),
        )
        .await
        .expect("subscribe");
    let publisher = broker.publisher(LapinPublish::default().exchange(&name).confirms());
    let plain = broker.publisher(LapinPublish::default());
    for round in 0..3 {
        if round > 0 {
            proxy.offline();
            proxy.wait_for(|| !broker.is_connected()).await;
            // Poll during the outage: the worker must neither end nor yield a transient error.
            assert!(
                tokio::time::timeout(
                    Duration::from_millis(150),
                    subscriber.stream().boxed().next()
                )
                .await
                .is_err()
            );
            assert!(
                publisher
                    .publish(OutgoingMessage::new("key", b"uncertain"), None)
                    .await
                    .is_err()
            );
            proxy.online();
            proxy.wait_for(|| broker.is_connected()).await;
        }
        // Connection readiness can precede channel restoration. Failed operations remain visible;
        // retry this known probe explicitly until the channel accepts a fresh publish.
        tokio::time::timeout(WAIT, async {
            loop {
                if publisher
                    .publish(OutgoingMessage::new("key", b"confirmed"), None)
                    .await
                    .is_ok()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("publish after recovery");
        plain
            .publish(OutgoingMessage::new(&name, b"plain"), None)
            .await
            .expect("same plain handle");
        let mut batches = Box::pin(subscriber.batches(NonZeroUsize::new(2).expect("nonzero")));
        let batch = tokio::time::timeout(WAIT, batches.next())
            .await
            .expect("batch timeout")
            .expect("stream stays open")
            .expect("batch");
        assert_eq!(batch.len(), 2);
        assert_eq!(batch[0].payload(), b"confirmed");
        assert_eq!(batch[1].payload(), b"plain");
        for msg in batch {
            msg.ack().await.expect("ack after recovery");
        }
        drop(batches);
        // The delay context still references the recovered subscriber channel.
        plain
            .publish(OutgoingMessage::new(&name, b"delayed"), None)
            .await
            .expect("publish delayed");
        let msg = message(&mut subscriber).await;
        msg.nack_after(Duration::from_millis(50))
            .await
            .expect("native delay");
        let msg = message(&mut subscriber).await;
        assert_eq!(msg.payload(), b"delayed");
        msg.ack().await.expect("ack delayed");
    }
    broker.shutdown().await.expect("shutdown");
    assert!(matches!(
        plain
            .publish(OutgoingMessage::new(&name, b"closed"), None)
            .await,
        Err(AmqpError::Closed { .. })
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rpc_dispatcher_survives_and_request_deadlines_do_not_restart() {
    let Some(url) = live::url("AMQP_TEST_URL") else {
        return;
    };
    let proxy = proxy::Proxy::new(&url).await;
    let broker = LapinBroker::new(&proxy.url)
        .auto_recover(true)
        .connect()
        .await
        .expect("connect");
    let responder = lapin::Connection::connect(&url, lapin::ConnectionProperties::default())
        .await
        .expect("responder");
    let channel = responder.create_channel().await.expect("channel");
    let name = unique();
    channel
        .queue_declare(
            name.as_str().into(),
            lapin::options::QueueDeclareOptions {
                exclusive: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .expect("queue");
    let mut requests = channel
        .basic_consume(
            name.as_str().into(),
            "".into(),
            lapin::options::BasicConsumeOptions {
                no_ack: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .expect("consume");
    let requester = broker.requester(LapinRequest::default());
    for round in 0..2 {
        let deadline = if round == 0 {
            Duration::from_millis(400)
        } else {
            WAIT
        };
        let ask = requester.request(OutgoingMessage::new(&name, b"ask"), deadline);
        let respond = async {
            let msg = tokio::time::timeout(WAIT, requests.next())
                .await
                .expect("request timeout")
                .expect("request")
                .expect("delivery");
            let reply_to = msg
                .properties
                .reply_to()
                .as_ref()
                .expect("reply-to")
                .clone();
            let id = msg
                .properties
                .correlation_id()
                .as_ref()
                .expect("correlation")
                .clone();
            if round == 0 {
                proxy.offline();
                proxy.wait_for(|| !broker.is_connected()).await;
                return;
            }
            channel
                .basic_publish(
                    "".into(),
                    reply_to,
                    lapin::options::BasicPublishOptions::default(),
                    b"reply",
                    lapin::BasicProperties::default().with_correlation_id(id),
                )
                .await
                .expect("reply publish");
        };
        if round == 0 {
            let (result, ()) = tokio::join!(ask, respond);
            assert!(matches!(result, Err(AmqpError::RequestTimeout(_))));
            proxy.online();
            proxy.wait_for(|| broker.is_connected()).await;
            tokio::time::sleep(Duration::from_millis(200)).await;
        } else {
            let (result, ()) = tokio::join!(ask, respond);
            assert_eq!(
                result.expect("same requester after recovery").payload(),
                b"reply"
            );
        }
    }
    broker.shutdown().await.expect("shutdown");
    responder
        .close(200, "OK".into())
        .await
        .expect("close responder");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_during_recovery_ends_streams_and_disables_aliases() {
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
    let name = unique();
    let mut subscriber = broker
        .subscribe(RabbitQueue::new(&name).durable(false).exclusive(true))
        .await
        .expect("subscribe");
    let publisher = broker.publisher(LapinPublish::default());
    let (original, _) = proxy.connections();
    proxy.offline();
    proxy.wait_for(|| !broker.is_connected()).await;
    tokio::time::timeout(Duration::from_secs(1), broker.shutdown())
        .await
        .expect("shutdown must not wait for reconnect")
        .expect("shutdown");
    assert!(
        tokio::time::timeout(Duration::from_secs(1), subscriber.stream().boxed().next())
            .await
            .expect("stream must end")
            .is_none()
    );
    proxy.online();
    proxy
        .wait_for(|| {
            let (accepted, active) = proxy.connections();
            accepted > original && active == 0
        })
        .await;
    assert!(matches!(
        publisher
            .publish(OutgoingMessage::new(&name, b"closed"), None)
            .await,
        Err(AmqpError::Closed { .. })
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recovery_is_disabled_by_default() {
    let Some(url) = live::url("AMQP_TEST_URL") else {
        return;
    };
    let proxy = proxy::Proxy::new(&url).await;
    let broker = LapinBroker::new(&proxy.url)
        .declare_topology(true)
        .connect()
        .await
        .expect("connect");
    let mut subscriber = broker
        .subscribe(RabbitQueue::new(unique()).durable(false).exclusive(true))
        .await
        .expect("subscribe");
    proxy.offline();
    proxy.wait_for(|| !broker.is_connected()).await;
    let mut stream = Box::pin(subscriber.stream());
    let first = tokio::time::timeout(WAIT, stream.next())
        .await
        .expect("terminal stream");
    assert!(first.is_none_or(|item| item.is_err()));
    assert!(
        tokio::time::timeout(WAIT, stream.next())
            .await
            .expect("ended stream")
            .is_none()
    );
    drop(stream);
    broker.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn classic_and_quorum_consumers_keep_their_prefetch_after_recovery() {
    let Some(url) = live::url("AMQP_TEST_URL") else {
        return;
    };
    let operator = lapin::Connection::connect(&url, lapin::ConnectionProperties::default())
        .await
        .expect("operator");
    let channel = operator.create_channel().await.expect("operator channel");
    for quorum in [false, true] {
        let proxy = proxy::Proxy::new(&url).await;
        let broker = LapinBroker::new(&proxy.url)
            .auto_recover(true)
            .declare_topology(true)
            .prefetch(ruststream::nonzero!(7))
            .connect()
            .await
            .expect("connect");
        let queue = unique();
        let mut subscriber = if quorum {
            broker
                .subscribe(RabbitQuorumQueue::new(&queue).prefetch(ruststream::nonzero!(2)))
                .await
                .expect("quorum consumer")
        } else {
            broker
                .subscribe(RabbitQueue::new(&queue).prefetch(ruststream::nonzero!(2)))
                .await
                .expect("classic consumer")
        };
        proxy.offline();
        proxy.wait_for(|| !broker.is_connected()).await;
        proxy.online();
        proxy.wait_for(|| broker.is_connected()).await;
        for _ in 0..6 {
            channel
                .basic_publish(
                    "".into(),
                    queue.as_str().into(),
                    lapin::options::BasicPublishOptions::default(),
                    b"prefetch",
                    lapin::BasicProperties::default(),
                )
                .await
                .expect("publish");
        }
        for _ in 0..3 {
            let mut stream = subscriber.stream().boxed();
            let first = tokio::time::timeout(WAIT, stream.next())
                .await
                .expect("first timeout")
                .expect("open")
                .expect("first");
            let second = tokio::time::timeout(WAIT, stream.next())
                .await
                .expect("second timeout")
                .expect("open")
                .expect("second");
            assert!(
                tokio::time::timeout(Duration::from_millis(150), stream.next())
                    .await
                    .is_err(),
                "descriptor prefetch must cap unacknowledged deliveries at two after recovery"
            );
            first.ack().await.expect("ack first");
            second.ack().await.expect("ack second");
        }
        drop(subscriber);
        broker.shutdown().await.expect("shutdown");
        channel
            .queue_delete(
                queue.as_str().into(),
                lapin::options::QueueDeleteOptions::default(),
            )
            .await
            .expect("cleanup");
    }
    operator
        .close(200, "OK".into())
        .await
        .expect("operator close");
}
