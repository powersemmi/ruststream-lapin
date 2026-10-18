//! A service that reaches `RabbitMQ` over TLS: an `amqps://` URI and one of the crate's TLS
//! features.
//!
//! ```text
//! cargo test --example lapin_broker_tls --features testing,tls-rustls-ring
//! ```

use serde::{Deserialize, Serialize};

use ruststream_lapin::prelude::*;

#[subscriber("orders")]
async fn place(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "order placed");
    HandlerOutcome::ack()
}

// --8<-- [start:broker]
/// The `amqps` scheme on the server's TLS port, 5671: the connection opens with a TLS
/// handshake.
fn broker() -> LapinBroker {
    LapinBroker::new("amqps://orders:secret@rabbit.example.com:5671/shop")
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker(), |b| {
        b.include(place);
    })
}
// --8<-- [end:broker]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_lapin::prelude::*;

    use super::{Order, app};

    #[tokio::test]
    async fn the_service_on_an_amqps_uri_places_an_order() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<LapinBroker>()
            .message(&Order { id: 7 })
            .publish()
            .await?;

        tb.broker::<LapinBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }
}
