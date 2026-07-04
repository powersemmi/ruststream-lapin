//! {{project-name}} - a RustStream service over a RabbitMQ topic exchange.
//!
//! Handlers live in `events`, wiring in `routes`; `#[ruststream::app]` generates `main`, so there
//! is no runtime boilerplate to maintain:
//!
//! - `cargo run -- run` (or `ruststream run`) starts the service until interrupted.
//! - `cargo run -- asyncapi gen` (or `ruststream asyncapi gen`) prints the AsyncAPI document.
//!
//! This service owns its queues, so it opts into `declare_topology(true)`: subscribing declares
//! the `events` topic exchange, the queue, and the `order.*` binding. Start a broker first, for
//! example `docker run -p 5672:5672 rabbitmq:4`.

mod events;
mod routes;

use ruststream::runtime::{App, AppInfo, RustStream};
use ruststream_lapin::LapinBroker;

/// Builds the service: one RabbitMQ broker (declaring its topology) with the events router mounted.
#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("{{project-name}}", "0.1.0")).with_broker(
        LapinBroker::new("amqp://localhost:5672")
            .declare_topology(true)
            .prefetch(32),
        |b| {
            let router = routes::events(b.broker());
            b.include_router(router);
        },
    )
}
