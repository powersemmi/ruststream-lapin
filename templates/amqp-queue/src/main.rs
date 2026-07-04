//! {{project-name}} - a RustStream service over a RabbitMQ work queue.
//!
//! Handlers live in `orders`, wiring in `routes`; `#[ruststream::app]` generates `main`, so there
//! is no runtime boilerplate to maintain:
//!
//! - `cargo run -- run` (or `ruststream run`) starts the service until interrupted.
//! - `cargo run -- asyncapi gen` (or `ruststream asyncapi gen`) prints the AsyncAPI document.
//!
//! `LapinBroker::new` is synchronous and does no I/O, so it slots into the builder; the runtime
//! opens the connection once at startup, before the subscriptions consume. Start a broker first,
//! for example `docker run -p 5672:5672 rabbitmq:4`, and create the `orders` queue.

mod orders;
mod routes;

use ruststream::runtime::{App, AppInfo, RustStream};
use ruststream_lapin::LapinBroker;

/// Builds the service: one RabbitMQ broker with the orders router mounted.
#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("{{project-name}}", "0.1.0")).with_broker(
        LapinBroker::new("amqp://localhost:5672").prefetch(32),
        |b| {
            let router = routes::orders(b.broker());
            b.include_router(router);
        },
    )
}
