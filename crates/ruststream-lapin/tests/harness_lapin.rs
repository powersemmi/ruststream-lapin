//! The publish steps under the test harness: a handler bounds its slot with
//! [`LapinPublishExt`](ruststream_lapin::LapinPublishExt), takes a step on the injected
//! publisher, and the publish is still attributed to the slot.
//!
//! The step resolves on the slot entry the body holds, not on the publisher one layer below it,
//! which is what keeps `tb.out::<Marker>()` seeing the message; the property rides the outgoing
//! headers, so the in-process transport carries it exactly as the real broker writes it onto the
//! AMQP frame.

#![cfg(feature = "testing")]

use ruststream::runtime::{Out, PublishExt};
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

// The body the `lapin_priority` example ships, narrowed to one named slot so the harness can
// assert per slot.
#[subscriber("orders")]
async fn ship(
    order: &Order,
    Out(shipments): Out<impl LapinPublishExt, Shipments>,
) -> HandlerOutcome {
    let shipment = Shipment { order_id: order.id };
    let sent = if order.expedited {
        shipments
            .with_priority(9)
            .message(&shipment)
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
            b.include(ship).out(Shipments, Publish::default()).build();
        });
    let tb = TestApp::start(app).await.expect("start");
    tb.broker::<LapinTestBroker>()
        .publish("orders", order)
        .await
        .expect("publish drives the handler to quiescence");
    tb
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stepped_slot_publish_stays_attributed_and_carries_the_property() {
    let tb = deliver(&Order {
        id: 7,
        expedited: true,
    })
    .await;

    let stepped = tb
        .out::<Shipments>()
        .decoded_as::<Shipment>()
        .assert_called_once()
        .with(&Shipment { order_id: 7 });
    let recorded = stepped.messages().first().expect("one recorded publish");
    assert_eq!(
        recorded.headers().get(PRIORITY_HEADER),
        Some(b"9".as_slice()),
        "the step's property must travel with the message the slot recorded"
    );

    tb.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_plain_slot_publish_carries_no_property() {
    let tb = deliver(&Order {
        id: 1,
        expedited: false,
    })
    .await;

    let plain = tb
        .out::<Shipments>()
        .decoded_as::<Shipment>()
        .assert_called_once();
    let recorded = plain.messages().first().expect("one recorded publish");
    assert_eq!(
        recorded.headers().get(PRIORITY_HEADER),
        None,
        "a publish without a step leaves the property off the message"
    );

    tb.shutdown().await.expect("shutdown");
}
