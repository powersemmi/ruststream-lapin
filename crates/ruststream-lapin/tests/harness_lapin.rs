//! The publish steps under the test harness: a handler adjusts an AMQP property for one message,
//! and the publish is still attributed to its slot.
//!
//! A step is a position on the publish builder, not a wrapper around the publisher, so
//! `tb.out::<Marker>()` sees the message either way. What the step asked for is read back with
//! `with_options`, and what reached the broker is read off the delivery a consumer gets, which
//! reports the priority under the header a live delivery reports it under.

#![cfg(feature = "testing")]

use std::convert::Infallible;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ruststream::runtime::Out;
use ruststream::testing::TestApp;
use ruststream::{OutSlot, Outgoing};
use ruststream_lapin::PRIORITY_HEADER;
use ruststream_lapin::prelude::*;
use serde::{Deserialize, Serialize};

/// The address the service's broker is built with; the in-process mode dials nothing.
const URI: &str = "amqp://localhost:5672";

#[derive(Debug, Outgoing, Serialize, Deserialize)]
struct Order {
    id: u64,
    expedited: bool,
}

#[derive(Debug, Outgoing, Serialize, Deserialize, PartialEq)]
#[outgoing(name = "shipments")]
struct Shipment {
    order_id: u64,
}

#[derive(OutSlot)]
#[publishes(Shipment)]
struct Shipments;

/// The mount site's own priority, which every shipment carries unless a step says otherwise.
const MOUNTED_PRIORITY: u8 = 3;

// The body the `lapin_priority` example ships, narrowed to one named slot so the harness can
// assert per slot.
#[subscriber("orders")]
async fn ship(
    order: &Order,
    Out(shipments): Out<impl Publisher<Options = LapinPublishOptions>, Shipments>,
) -> HandlerOutcome {
    let shipment = Shipment { order_id: order.id };
    let sent = if order.expedited {
        shipments
            .message(&shipment)
            .priority(9)
            .expiration(Duration::from_secs(3600))
            .publish()
            .await
    } else {
        shipments.message(&shipment).publish().await
    };
    if sent.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

/// The priority each shipment delivery reported, in delivery order.
#[derive(Clone, Default)]
struct Priorities(Arc<Mutex<Vec<Option<String>>>>);

impl Priorities {
    fn seen(&self) -> Vec<Option<String>> {
        self.0.lock().expect("priorities mutex poisoned").clone()
    }
}

/// The consumer of the shipments: it records the priority its delivery reports.
#[subscriber("shipments")]
async fn receive(shipment: &Shipment, ctx: &mut Context<'_, (), Priorities>) -> HandlerOutcome {
    let _ = shipment.order_id;
    let priority = ctx.headers().get_str(PRIORITY_HEADER).map(str::to_owned);
    ctx.state()
        .0
        .lock()
        .expect("priorities mutex poisoned")
        .push(priority);
    HandlerOutcome::ack()
}

/// Starts the app, delivers one order, and hands back the running harness with what the
/// shipments consumer saw.
async fn deliver(order: &Order) -> (TestApp<Priorities>, Priorities) {
    let priorities = Priorities::default();
    let seen = priorities.clone();
    let app = RustStream::new(AppInfo::new("orders", "0.1.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(priorities))
        .with_broker(LapinBroker::new(URI), |b| {
            b.include(ship)
                .out(Shipments, Publish::default().priority(MOUNTED_PRIORITY))
                .build();
            b.include(receive);
        });
    let tb = TestApp::start(app).await.expect("start");
    tb.broker::<LapinBroker>()
        .message(order)
        .to("orders")
        .publish()
        .await
        .expect("publish drives the handler to quiescence");
    (tb, seen)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stepped_slot_publish_stays_attributed_and_carries_what_the_step_asked_for() {
    let (tb, priorities) = deliver(&Order {
        id: 7,
        expedited: true,
    })
    .await;

    let stepped = tb
        .out::<Shipments>()
        .assert_called_once()
        .with_options(&LapinPublishOptions {
            priority: Some(9),
            expiration: Some(Duration::from_secs(3600)),
            persistent: None,
        });
    stepped
        .decoded_as::<Shipment>()
        .with(&Shipment { order_id: 7 });

    // The delivery carries the resolved property, which is where a consumer reads it.
    assert_eq!(priorities.seen(), vec![Some("9".to_owned())]);

    tb.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_plain_slot_publish_takes_the_priority_the_mount_site_fixed() {
    let (tb, priorities) = deliver(&Order {
        id: 1,
        expedited: false,
    })
    .await;

    tb.out::<Shipments>()
        .assert_called_once()
        .assert_options_default()
        .decoded_as::<Shipment>()
        .with(&Shipment { order_id: 1 });

    assert_eq!(priorities.seen(), vec![Some(MOUNTED_PRIORITY.to_string())]);

    tb.shutdown().await.expect("shutdown");
}
