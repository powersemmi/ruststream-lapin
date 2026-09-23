//! The macro-free path against this broker: a `Handle` body bound to a `RabbitQueue` descriptor
//! by `subscriber(source, body)`.
//!
//! What the attribute writes for a service, a broker crate has to make available to a hand-written
//! definition too: the descriptor is the subscription source, the crate's typed per-delivery
//! context is the body's `C` axis, and an injected slot publishes through the policy the include
//! site attaches. The `#[subscriber]` cases live in `tests/in_process_lapin.rs`.

#![cfg(feature = "testing")]

use ruststream::prelude::*;
// The payload schemas the generated document reports: the manual path asks its message types for
// them at the mount, where the attribute path captures them on its own. The derive is the core's
// own re-export, so this test needs no schemars of its own to keep in step with it.
use ruststream::Outgoing;
#[cfg(feature = "asyncapi")]
use ruststream::schemars::JsonSchema;
use ruststream::testing::TestApp;
use ruststream_lapin::context::AmqpContext;
use ruststream_lapin::context::keys::RoutingKey;
use ruststream_lapin::{LapinBroker, LapinPublish, RabbitQueue};

use serde::{Deserialize, Serialize};

/// The address the service's broker is built with; the in-process mode dials nothing.
const URI: &str = "amqp://localhost:5672";

#[cfg_attr(feature = "asyncapi", derive(JsonSchema))]
#[cfg_attr(feature = "asyncapi", schemars(crate = "ruststream::schemars"))]
#[derive(Debug, Deserialize, Serialize, PartialEq, Outgoing)]
struct Order {
    id: u64,
}

#[cfg_attr(feature = "asyncapi", derive(JsonSchema))]
#[cfg_attr(feature = "asyncapi", schemars(crate = "ruststream::schemars"))]
#[derive(Debug, Outgoing, Serialize)]
#[outgoing(name = "orders.audit")]
struct Audited {
    id: u64,
    via: String,
}

/// A body over its injected publisher and this crate's per-delivery context: the arena is
/// described by its entry rather than by the mount's wiring, so the definition is a zero-sized
/// value the mount site builds for free and the signature survives a mount that adds a step.
struct Audit;

impl<Egress> Handle<Order, (), Outs<(Egress,)>, AmqpContext> for Audit
where
    Egress: OutEntry<DefaultSlot, Wire: Publisher>,
{
    async fn handle(
        &self,
        order: &Order,
        outs: &Outs<(Egress,)>,
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
    let app =
        RustStream::new(AppInfo::new("audit", "0.1.0")).with_broker(LapinBroker::new(URI), |b| {
            b.include(subscriber(RabbitQueue::new("orders"), Audit).build())
                .out(DefaultSlot, LapinPublish::default())
                .build();
        });
    let tb = TestApp::start(app).await.expect("start");

    tb.broker::<LapinBroker>()
        .message(&Order { id: 7 })
        .to("orders")
        .publish()
        .await
        .expect("publish drives the handler to quiescence");

    // The body reads the delivery's routing key off the typed context, and the slot publish
    // reaches the transport.
    tb.broker::<LapinBroker>()
        .published::<()>("orders.audit")
        .assert_called_once()
        .with_raw(br#"{"id":7,"via":"orders"}"#);

    tb.broker::<LapinBroker>()
        .subscriber("orders")
        .assert_called_once()
        .settled(HandlerOutcome::ack());

    tb.shutdown().await.expect("shutdown");
}
