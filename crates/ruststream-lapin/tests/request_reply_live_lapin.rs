//! Request/reply against a real `RabbitMQ`: the mount-site wiring a service writes, over direct
//! reply-to.
//!
//! `tests/request_reply_lapin.rs` runs the same pair in process, where the transport reproduces
//! the convention. What only a server has is `amq.rabbitmq.reply-to`: a pseudo-queue the broker
//! rewrites per request and routes the answer back over the requester's own channel, with no
//! queue declared anywhere. So the responder here is an ordinary handler carrying the crate's
//! transform on its reply publisher, and the caller is a live requester.
//!
//! Every test is a no-op unless `AMQP_TEST_URL` points at a broker (see `just test-brokers`).

use std::sync::Arc;
use std::time::Duration;

use futures::{Stream, StreamExt};
use lapin::options::QueueDeleteOptions;
use lapin::{Connection, ConnectionProperties};
use ruststream::{
    Broker, ConnectedBroker, IncomingMessage, OutgoingMessage, Publisher, Subscriber,
};
use ruststream_lapin::prelude::*;
use ruststream_lapin::{AmqpError, LapinMessage};
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

mod live;

const WAIT: Duration = Duration::from_secs(10);

/// The queue the responder consumes, and the name its replies fall back to. The attribute takes
/// a literal or a `&'static str` constant, so these are fixed names the test cleans up after
/// itself.
const ASK: &str = "ruststream-rpc.inventory.check";
const UNROUTED: &str = "ruststream-rpc.inventory.unrouted";

/// The broker address, or `None` to skip. Under `RUSTSTREAM_REQUIRE_LIVE` a missing address
/// fails the suite instead of skipping it.
fn amqp_url() -> Option<String> {
    live::url("AMQP_TEST_URL")
}

#[derive(Deserialize)]
struct CheckStock {
    sku: String,
}

/// An RPC reply goes wherever the request asked, so the type declares no destination of its own.
#[derive(Deserialize, Serialize, Outgoing)]
struct Stock {
    available: bool,
}

#[subscriber(RabbitQueue::new(ASK), publish(UNROUTED))]
async fn check(ask: &CheckStock) -> Stock {
    Stock {
        available: ask.sku != "unobtainium",
    }
}

async fn next<S>(stream: &mut S) -> LapinMessage
where
    S: Stream<Item = Result<LapinMessage, AmqpError>> + Unpin,
{
    tokio::time::timeout(WAIT, stream.next())
        .await
        .expect("delivery within timeout")
        .expect("stream has next")
        .expect("delivery ok")
}

/// Declares `queue` and goes away, so a request published before the responder is up waits there
/// instead of being dropped by the default exchange.
async fn declare_and_leave(url: &str, queue: &str) {
    let broker = LapinBroker::new(url.to_owned())
        .declare_topology(true)
        .connect()
        .await
        .expect("declare connect");
    let subscriber = broker
        .subscribe(RabbitQueue::new(queue))
        .await
        .expect("declare");
    drop(subscriber);
    broker.shutdown().await.expect("declare shutdown");
}

/// Removes the fixed queues, before a run and after it.
async fn delete_queues(url: &str, queues: &[&str]) {
    let cleanup = Connection::connect(url, ConnectionProperties::default())
        .await
        .expect("cleanup connect");
    let channel = cleanup.create_channel().await.expect("cleanup channel");
    for queue in queues {
        let _ = channel
            .queue_delete((*queue).into(), QueueDeleteOptions::default())
            .await;
    }
    cleanup
        .close(200, "OK".into())
        .await
        .expect("cleanup close");
}

// Both directions of the responder wiring on a server: a delivery that carries a reply address is
// answered there, and one that carries none falls through to the name the mount site declared.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_responder_answers_the_requester_and_falls_back_to_the_mount_site_name() {
    let Some(url) = amqp_url() else { return };
    delete_queues(&url, &[ASK, UNROUTED]).await;
    declare_and_leave(&url, ASK).await;

    let client = LapinBroker::new(url.clone())
        .declare_topology(true)
        .connect()
        .await
        .expect("connect");
    let mut unrouted = client
        .subscribe(RabbitQueue::new(UNROUTED))
        .await
        .expect("subscribe the fallback queue");

    let stop = Arc::new(Notify::new());
    let waiter = Arc::clone(&stop);
    let app = RustStream::new(AppInfo::new("inventory", "0.1.0")).with_broker(
        LapinBroker::new(url.clone()).declare_topology(true),
        |b| {
            b.include(check)
                .out_reply(Publish::default())
                .transform(DirectReplyTo);
        },
    );
    let running = tokio::spawn(app.run_until(async move { waiter.notified().await }));

    // The request carries the private reply address and a correlation id, and the transform sends
    // the answer to exactly that address. The timeout covers the responder's startup, because the
    // request waits in the queue until it is up.
    let requester = client.requester(Request::default());
    let reply = requester
        .request(
            OutgoingMessage::new(ASK, br#"{"sku":"widget"}"#),
            Duration::from_secs(15),
        )
        .await
        .expect("the responder must answer the address the request named");
    let stock: Stock = serde_json::from_slice(reply.payload()).expect("decode the reply");
    assert!(
        stock.available,
        "the reply must be this responder's answer to this request"
    );

    // A delivery with no reply address is not an RPC: the transform leaves the destination alone
    // and the reply goes where the mount site said.
    client
        .publisher(Publish::default())
        .publish(OutgoingMessage::new(ASK, br#"{"sku":"unobtainium"}"#), None)
        .await
        .expect("publish a request with no reply address");
    let mut fallback = Box::pin(unrouted.stream());
    let answered = next(&mut fallback).await;
    let stock: Stock =
        serde_json::from_slice(answered.payload()).expect("decode the fallback reply");
    assert!(
        !stock.available,
        "the fallback must carry this responder's answer, not a stale message"
    );
    answered.ack().await.expect("ack");

    stop.notify_one();
    tokio::time::timeout(WAIT, running)
        .await
        .expect("the responder shuts down")
        .expect("app task did not panic")
        .expect("run_until succeeded");

    drop(fallback);
    drop(unrouted);
    client.shutdown().await.expect("shutdown");
    delete_queues(&url, &[ASK, UNROUTED]).await;
}
