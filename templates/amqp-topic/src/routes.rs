//! Wiring: collect the `events` handlers into one `Router`, mounted by `main` via `include_router`.
//!
//! Keeping registration in its own module lets the handlers stay broker-agnostic - the router binds
//! to a concrete broker only when `main` mounts it.

use ruststream::runtime::RouterDef;
use ruststream_lapin::prelude::*;

use crate::events;

/// Builds the events router: a recording handler that replies to the `events` topic exchange, plus
/// a plain shipment handler.
///
/// `record` replies through a policy targeting the `events` exchange (so the reply's routing key
/// `order.recorded` is matched by topic bindings, not treated as a queue name). `TypedPublisher::new`
/// pairs it with the default codec, reused to decode the event. The policy holds no connection, so
/// the router is built long before anything connects and the runtime pairs it at startup. Naming
/// the reply publisher is what commits that registration; `on_shipment` has no reply, so `include`
/// commits it on its own. The router is a consuming builder, so the calls chain.
pub fn events() -> impl RouterDef<LapinBroker> {
    let recorded = TypedPublisher::new(Publish::default().exchange("events"));

    Router::new()
        .include(events::record)
        .publisher(recorded)
        .include(events::on_shipment)
}
