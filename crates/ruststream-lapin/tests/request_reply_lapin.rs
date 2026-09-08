//! Request/reply on the in-process transport: the RPC pair a service ships, mounted on the test
//! broker with the same policies it mounts on `RabbitMQ`.
//!
//! The stand-in reproduces the convention, not the transport: a private reply address per
//! request, a generated correlation id, a reply that does not correlate dropped rather than
//! resolving the call, and a timeout when nobody answers. What it cannot reproduce - direct
//! reply-to being at-most-once channel state on one broker node - is covered against a live
//! server by `tests/integration_lapin.rs`.

#![cfg(feature = "testing")]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use ruststream::codec::{Codec, JsonCodec};
use ruststream::testing::TestApp;
use ruststream::{
    Broker, ConnectedBroker, FromRef, HeaderMap, IncomingMessage, OutgoingMessage, Publisher,
    Subscriber,
};
use ruststream_lapin::prelude::*;
use ruststream_lapin::testing::LapinTestBroker;
use ruststream_lapin::{AmqpError, LapinPublish, LapinRequest};
use serde::{Deserialize, Serialize};

const RPC_TIMEOUT: Duration = Duration::from_secs(1);
/// Short enough to keep the negative cases quick, long enough not to race a busy runner.
const MISS_TIMEOUT: Duration = Duration::from_millis(200);

#[derive(Debug, Deserialize, Serialize)]
struct Order {
    sku: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct CheckStock {
    sku: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct Stock {
    available: bool,
}

/// What the ordering handler concluded, so the test can read the RPC result out of the app.
#[derive(Clone, Default)]
struct Decisions(Arc<Mutex<Vec<String>>>);

#[derive(Clone, FromRef)]
struct RpcState {
    decisions: Decisions,
}

// The responder: an ordinary handler whose reply is redirected to the requester's private
// address by the crate's own transform.
#[subscriber("inventory.check", publish("inventory.unrouted"))]
async fn check_stock(ask: &CheckStock) -> Stock {
    Stock {
        available: ask.sku != "unobtainium",
    }
}

// The requester: a handler that calls the other service in the middle of its own message flow,
// bounded by the capability alone, so the same body mounts on a server.
#[subscriber("orders")]
async fn place_order(
    order: &Order,
    Out(inventory): Out<impl RequestReply>,
    State(decisions): State<Decisions>,
) -> HandlerOutcome {
    let payload = JsonCodec
        .encode(&CheckStock {
            sku: order.sku.clone(),
        })
        .expect("the request encodes");
    let decision = match inventory
        .request(
            OutgoingMessage::new("inventory.check", payload.as_ref()),
            RPC_TIMEOUT,
        )
        .await
    {
        Ok(reply) => {
            let stock: Stock = JsonCodec
                .decode(reply.payload())
                .expect("the reply decodes");
            if stock.available {
                "accepted".to_owned()
            } else {
                "rejected".to_owned()
            }
        }
        Err(err) => format!("unavailable: {err}"),
    };
    decisions
        .0
        .lock()
        .expect("decisions mutex poisoned")
        .push(decision);
    HandlerOutcome::ack()
}

/// Starts the RPC pair in one app and hands back the harness plus the decisions it records.
async fn rpc_pair() -> (TestApp<RpcState>, Decisions) {
    let decisions = Decisions::default();
    let recorded = decisions.clone();
    let app = RustStream::new(AppInfo::new("orders", "0.1.0"))
        .on_startup(
            move |()| async move { Ok::<_, std::convert::Infallible>(RpcState { decisions }) },
        )
        .with_broker(LapinTestBroker::new(), |b| {
            b.include(place_order)
                .out(DefaultSlot, Request::default())
                .build();
            b.include(check_stock)
                .out(Reply, Publish::default())
                .transform(DirectReplyTo);
        });
    (TestApp::start(app).await.expect("start"), recorded)
}

// The definition of done for the capability: a handler bounded `Out<impl RequestReply>` mounts on
// the stand-in through the production policy, and the call really resolves through a responder
// running in the same app.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_handler_binding_request_reply_mounts_and_gets_its_answer() {
    let (tb, decisions) = rpc_pair().await;

    tb.broker::<LapinTestBroker>()
        .publish(
            "orders",
            &Order {
                sku: "widget".to_owned(),
            },
        )
        .await
        .expect("publish must drive the RPC round trip to quiescence");
    tb.broker::<LapinTestBroker>()
        .publish(
            "orders",
            &Order {
                sku: "unobtainium".to_owned(),
            },
        )
        .await
        .expect("publish must drive the RPC round trip to quiescence");

    assert_eq!(
        decisions
            .0
            .lock()
            .expect("decisions mutex poisoned")
            .clone(),
        vec!["accepted".to_owned(), "rejected".to_owned()],
        "each order is settled by the reply its own request received"
    );
    tb.broker::<LapinTestBroker>()
        .subscriber("orders")
        .assert_called(2)
        .settled(HandlerOutcome::ack());
    tb.broker::<LapinTestBroker>()
        .subscriber("inventory.check")
        .assert_called(2);

    tb.shutdown().await.expect("shutdown");
}

// The negative case a service codes against: nobody consumes the request queue, so the call
// fails on its own deadline instead of hanging.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unanswered_request_times_out() {
    let broker = LapinTestBroker::new().connect().await.expect("connect");
    let requester = broker.requester(LapinRequest::default());

    let asked = requester
        .request(OutgoingMessage::new("inventory.check", b"{}"), MISS_TIMEOUT)
        .await;

    assert!(
        matches!(asked, Err(AmqpError::RequestTimeout(waited)) if waited == MISS_TIMEOUT),
        "an unanswered request must time out, got: {asked:?}"
    );
    broker.shutdown().await.expect("shutdown");
}

// Correlation is what makes a reply THIS request's answer: a message that lands on the reply
// address without the id the request carried is not one, and the request keeps waiting.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reply_that_does_not_correlate_is_dropped() {
    let broker = LapinTestBroker::new().connect().await.expect("connect");
    let mut responder = broker
        .subscribe("inventory.check")
        .await
        .expect("subscribe");
    let publisher = broker.publisher(LapinPublish::default());
    let requester = broker.requester(LapinRequest::default());

    let respond = async {
        let mut stream = std::pin::pin!(responder.stream());
        let ask = futures::StreamExt::next(&mut stream)
            .await
            .expect("a request arrives")
            .expect("delivery ok");
        let reply_to = ask
            .headers()
            .reply_to()
            .expect("a request carries its private reply address")
            .to_owned();
        let mut headers = HeaderMap::new();
        headers.insert("correlation-id", "someone-elses-question");
        publisher
            .publish(OutgoingMessage::new(&reply_to, b"{}").with_headers(headers))
            .await
            .expect("reply publish");
        ack(ask).await;
    };
    let asked = requester.request(OutgoingMessage::new("inventory.check", b"{}"), MISS_TIMEOUT);

    let (asked, ()) = futures::join!(asked, respond);
    assert!(
        matches!(asked, Err(AmqpError::RequestTimeout(_))),
        "an uncorrelated reply must not resolve the request, got: {asked:?}"
    );
    broker.shutdown().await.expect("shutdown");
}

// Every request gets a reply address of its own, as `RabbitMQ` rewrites direct reply-to per
// request; a responder that kept an address from an earlier question must not reach this one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn each_request_carries_its_own_reply_address() {
    let broker = LapinTestBroker::new().connect().await.expect("connect");
    let mut responder = broker
        .subscribe("inventory.check")
        .await
        .expect("subscribe");
    let requester = broker.requester(LapinRequest::default());

    let observe = async {
        let mut stream = std::pin::pin!(responder.stream());
        let mut addresses = Vec::new();
        for _ in 0..2 {
            let ask = futures::StreamExt::next(&mut stream)
                .await
                .expect("a request arrives")
                .expect("delivery ok");
            addresses.push((
                ask.headers()
                    .reply_to()
                    .expect("a private reply address")
                    .to_owned(),
                ask.headers()
                    .correlation_id()
                    .expect("a correlation id")
                    .to_owned(),
            ));
            ack(ask).await;
        }
        addresses
    };
    let ask = async {
        // Both requests go unanswered on purpose: this is about what they carry, not about what
        // comes back.
        for _ in 0..2 {
            let _ = requester
                .request(OutgoingMessage::new("inventory.check", b"{}"), MISS_TIMEOUT)
                .await;
        }
    };

    let (addresses, ()) = futures::join!(observe, ask);
    assert_ne!(
        addresses[0], addresses[1],
        "two requests must not share a reply address or a correlation id"
    );
    assert!(
        addresses
            .iter()
            .all(|(address, _)| address.starts_with("amq.rabbitmq.reply-to.")),
        "the reply address must be the direct reply-to pseudo-queue the responder expects, got: \
         {addresses:?}"
    );
    broker.shutdown().await.expect("shutdown");
}

// A requester aliases the transport and may outlive it, exactly like a publisher paired before
// the shutdown.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn requesting_after_shutdown_errors() {
    let broker = LapinTestBroker::new().connect().await.expect("connect");
    let requester = broker.requester(LapinRequest::default());
    broker.shutdown().await.expect("shutdown");

    let asked = requester
        .request(OutgoingMessage::new("inventory.check", b"{}"), MISS_TIMEOUT)
        .await;

    assert!(
        matches!(asked, Err(AmqpError::Closed { .. })),
        "a request after shutdown must report the closed transport, got: {asked:?}"
    );
}

/// Settles a delivery the test is done with; the in-process ack never fails.
async fn ack(msg: impl IncomingMessage) {
    msg.ack().await.expect("ack");
}
