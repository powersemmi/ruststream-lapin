//! The publish steps under the test harness: a handler adjusts an AMQP property for one message,
//! and the publish is still attributed to its slot.
//!
//! A step is a position on the publish builder, not a wrapper around the publisher, so
//! `tb.out::<Marker>()` sees the message either way. What the step asked for is read back with
//! `with_options`, and what the mount site fixed is read off the delivery, where the transport
//! reports it under the header a real delivery reports it under.

#![cfg(feature = "testing")]

use std::time::Duration;

use ruststream::runtime::Out;
use ruststream::testing::TestApp;
use ruststream::{OutSlot, Outgoing};
use ruststream_lapin::PRIORITY_HEADER;
use ruststream_lapin::prelude::*;
use ruststream_lapin::testing::LapinTestBroker;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
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

/// Starts the app, delivers one order, and hands back the running harness.
async fn deliver(order: &Order) -> TestApp<()> {
    let app =
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(LapinTestBroker::new(), |b| {
            b.include(ship)
                .out(Shipments, Publish::default().priority(MOUNTED_PRIORITY))
                .build();
        });
    let tb = TestApp::start(app).await.expect("start");
    tb.broker::<LapinTestBroker>()
        .publish("orders", order)
        .await
        .expect("publish drives the handler to quiescence");
    tb
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stepped_slot_publish_stays_attributed_and_carries_what_the_step_asked_for() {
    let tb = deliver(&Order {
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
    tb.broker::<LapinTestBroker>()
        .published::<Shipment>("shipments")
        .assert_called_once()
        .with_header(PRIORITY_HEADER, "9");

    tb.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_plain_slot_publish_takes_the_priority_the_mount_site_fixed() {
    let tb = deliver(&Order {
        id: 1,
        expedited: false,
    })
    .await;

    tb.out::<Shipments>()
        .assert_called_once()
        .assert_options_default()
        .decoded_as::<Shipment>()
        .with(&Shipment { order_id: 1 });

    tb.broker::<LapinTestBroker>()
        .published::<Shipment>("shipments")
        .assert_called_once()
        .with_header(PRIORITY_HEADER, MOUNTED_PRIORITY.to_string());

    tb.shutdown().await.expect("shutdown");
}
