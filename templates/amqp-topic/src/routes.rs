//! Wiring: collect the `events` handlers into one `Router`, mounted by `main` via `include_router`.
//!
//! Keeping registration in its own module lets the handlers stay broker-agnostic - the router binds
//! to a concrete broker only when `main` mounts it.

use ruststream::runtime::{Router, RouterDef, TypedPublisher};
use ruststream_lapin::LapinBroker;

use crate::events;

/// Builds the events router: a recording handler that replies to the `events` topic exchange, plus
/// a plain shipment handler.
///
/// `record` replies through a publisher targeting the `events` exchange (so the reply's routing key
/// `order.recorded` is matched by topic bindings, not treated as a queue name). `TypedPublisher::new`
/// pairs it with the default codec, reused to decode the event. The router is a consuming builder,
/// so the calls chain; `use<>` opts out of borrowing `broker`, so `main` can still mount it.
pub fn events(broker: &LapinBroker) -> impl RouterDef<LapinBroker> + use<> {
    let recorded = TypedPublisher::new(broker.publisher().exchange("events"));

    Router::new()
        .include_publishing(events::record, recorded)
        .include(events::on_shipment)
}
