//! Wiring: collect the `orders` handlers into one `Router`, mounted by `main` via `include_router`.
//!
//! Keeping registration in its own module lets the handlers stay broker-agnostic - the router binds
//! to a concrete broker only when `main` mounts it.

use ruststream::runtime::RouterDef;
use ruststream_lapin::prelude::*;

use crate::orders;

/// Builds the orders router: a publishing handler (replies to the `confirmations` queue) plus a
/// plain one.
///
/// `confirm` needs a publisher for its reply, bound on the mount site with `out(Reply, ..)`; with
/// no codec named after it the reply takes the default one, which also decodes the order. The
/// policy holds no connection, so the router is built long before anything connects and the runtime
/// pairs it at startup. `build` seals that reply wiring and commits the registration; `on_cancel`
/// has no reply to wire, so `include` commits it on its own. The router is a consuming builder, so
/// the calls chain; the registration list is opaque, hence `impl RouterDef`.
pub fn orders() -> impl RouterDef<LapinBroker> {
    Router::new()
        .include(orders::confirm)
        .out(Reply, Publish::default())
        .build()
        .include(orders::on_cancel)
}
