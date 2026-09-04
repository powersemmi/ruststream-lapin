//! The macro-free path against this broker: a `Handle` body bound to a `RabbitQueue` descriptor
//! by `subscriber(source, body)`.
//!
//! What the attribute writes for a service, a broker crate has to make available to a hand-written
//! definition too: the descriptor is the subscription source, the crate's typed per-delivery
//! context is the body's `C` axis, and an injected slot publishes through the policy the include
//! site attaches. The `#[subscriber]` cases live in `tests/testing_core.rs`.

#![cfg(feature = "testing")]

use std::time::Duration;

use ruststream::codec::Codec;
use ruststream::prelude::*;
use ruststream::testing::{TestApp, expect_published};
use ruststream::{Broker, ConnectedBroker, Outgoing};
use ruststream_lapin::RabbitQueue;
use ruststream_lapin::context::AmqpContext;
use ruststream_lapin::context::keys::RoutingKey;
use ruststream_lapin::testing::{LapinTestBroker, LapinTestPublish};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, PartialEq)]
struct Order {
    id: u64,
}

#[derive(Debug, Outgoing, Serialize)]
#[outgoing(name = "orders.audit")]
struct Audited {
    id: u64,
    via: String,
}

/// A body over its injected publisher and this crate's per-delivery context: both axes stay
/// generic in the impl, so the definition is a zero-sized value the mount site builds for free.
struct Audit;

impl<Egress, Enc> Handle<Order, (), Outs<(Slot<DefaultSlot, Egress, Enc>,)>, AmqpContext> for Audit
where
    Egress: Publisher,
    Enc: Codec + Send + Sync,
{
    async fn handle(
        &self,
        order: &Order,
        outs: &Outs<(Slot<DefaultSlot, Egress, Enc>,)>,
        ctx: &mut Context<'_, AmqpContext>,
    ) -> Result<(), HandlerOutcome> {
        let audited = Audited {
            id: order.id,
            via: ctx.context(RoutingKey).to_owned(),
        };
        if outs
            .get(DefaultSlot)
            .message(&audited)
            .publish()
            .await
            .is_err()
        {
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
}

// The descriptor is the manual constructor's source, exactly as the decorator takes it, and the
// per-delivery context resolves against the delivery the descriptor's subscription yields.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_handle_body_mounts_on_the_queue_descriptor() {
    let broker = LapinTestBroker::new();
    let probe = broker.clone().connect().await.expect("connect");

    let app = RustStream::new(AppInfo::new("audit", "0.1.0")).with_broker(broker, |b| {
        b.include(subscriber(RabbitQueue::new("orders"), Audit).build())
            .publisher(LapinTestPublish);
    });
    let tb = TestApp::start(app).await.expect("start");

    tb.broker::<LapinTestBroker>()
        .publish("orders", &Order { id: 7 })
        .await
        .expect("publish drives the handler to quiescence");

    let audited = expect_published(&probe, "orders.audit", 1, Duration::from_secs(1)).await;
    assert_eq!(audited.len(), 1, "the slot publish reaches the transport");
    assert_eq!(
        audited[0].payload(),
        br#"{"id":7,"via":"orders"}"#.as_slice(),
        "the body reads the delivery's routing key off the typed context"
    );

    tb.broker::<LapinTestBroker>()
        .subscriber("orders")
        .assert_called_once()
        .settled(HandlerOutcome::ack());

    tb.shutdown().await.expect("shutdown");
    probe.shutdown().await.expect("probe shutdown");
}
