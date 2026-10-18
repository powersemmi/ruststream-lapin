//! The orders service of the Running in production guide: the connection URI comes from the
//! environment, the connection carries the service's name, and operations own the queues.
//!
//! ```text
//! AMQP_URL=amqp://guest:guest@localhost:5672/%2f \
//!   cargo run --example guide_production --features testing,ruststream/logging -- run
//! ```

use std::env;

use ruststream_lapin::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
struct Order {
    id: u64,
    amount_cents: u64,
}

#[subscriber(RabbitQueue::new("orders"))]
async fn take_order(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, order.amount_cents, "order taken");
    HandlerOutcome::ack()
}

// --8<-- [start:broker]
/// The broker as production runs it.
fn broker(uri: String) -> LapinBroker {
    LapinBroker::new(uri)
        // What the management UI lists for this service's connection.
        .connection_name(concat!("orders ", env!("CARGO_PKG_VERSION")))
        // At most 64 unacknowledged deliveries per subscription.
        .prefetch(nonzero!(64))
}

fn service(broker: LapinBroker) -> RustStream {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(take_order);
    })
}

#[ruststream::app]
fn app() -> impl App {
    // amqps://orders:secret@rabbit.internal:5671/shop in production.
    let uri = env::var("AMQP_URL").unwrap_or_else(|_| "amqp://localhost:5672".to_owned());
    service(broker(uri))
}
// --8<-- [end:broker]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_lapin::prelude::*;

    use super::{Order, broker, service};

    #[tokio::test]
    async fn the_service_starts_from_a_tls_uri_with_credentials() -> Result<(), Box<dyn Error>> {
        let uri = "amqps://orders:secret@rabbit.internal:5671/shop".to_owned();
        let tb = TestApp::start(service(broker(uri))).await?;

        tb.broker::<LapinBroker>()
            .message(&Order {
                id: 1,
                amount_cents: 500,
            })
            .to("orders")
            .publish()
            .await?;

        tb.broker::<LapinBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_malformed_uri_stops_the_start() {
        let uri = "amqp://rabbit.internal:not-a-port".to_owned();

        let started = TestApp::start(service(broker(uri))).await;

        assert!(
            started.is_err(),
            "a URI the connection cannot parse must not start"
        );
    }
}
