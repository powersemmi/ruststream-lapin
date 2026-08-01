//! Wiring: collect the `orders` handlers into one `Router`, mounted by `main` via `include_router`.
//!
//! Keeping registration in its own module lets the handlers stay broker-agnostic - the router binds
//! to a concrete broker only when `main` mounts it.

use ruststream::runtime::{Router, RouterDef, TypedPublisher};
use ruststream_lapin::{LapinBroker, LapinPublish};

use crate::orders;

/// Builds the orders router: a publishing handler (replies to the `confirmations` queue) plus a
/// plain one.
///
/// `confirm` needs a publisher for its reply; `TypedPublisher::new` pairs the publish policy with
/// the default codec, reused to decode the order. The policy holds no connection, so the router is
/// built long before anything connects and the runtime pairs it at startup. `on_cancel` has no
/// reply, so it is mounted with `include`. The router is a consuming builder, so the calls chain;
/// the registration list is opaque, hence `impl RouterDef`.
pub fn orders() -> impl RouterDef<LapinBroker> {
    let confirmations = TypedPublisher::new(LapinPublish::default());

    Router::new()
        .include_publishing(orders::confirm, confirmations)
        .include(orders::on_cancel)
}
