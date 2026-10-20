//! The connection of a service: the URI with its credentials and virtual host, and the name the
//! management UI shows for it.
//!
//! ```text
//! cargo test --example lapin_broker_connection --features testing
//! ```

use serde::{Deserialize, Serialize};

use ruststream_lapin::prelude::*;

#[subscriber("orders")]
async fn place(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "order placed");
    HandlerOutcome::ack()
}

// --8<-- [start:broker]
/// The user `orders` on the virtual host `shop`; the management UI lists the connection as
/// `orders-svc`.
fn broker() -> LapinBroker {
    LapinBroker::new("amqp://orders:secret@localhost:5672/shop?heartbeat=30")
        .connection_name("orders-svc")
        .auto_recover(true)
        .recovery_retries(32)
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
    use ruststream_lapin::AmqpError;
    use ruststream_lapin::prelude::*;

    use super::{Order, app, place};

    #[tokio::test]
    async fn the_service_connects_and_places_an_order() -> Result<(), Box<dyn Error>> {
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

    #[tokio::test]
    async fn an_address_without_a_scheme_fails_startup() {
        let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
            LapinBroker::new("orders:secret@localhost:5672/shop"),
            |b| {
                b.include(place);
            },
        );

        let failed = TestApp::start(app).await.err();
        let mut cause: Option<&(dyn Error + 'static)> = failed.as_ref().map(|err| err as _);
        while let Some(err) = cause {
            if let Some(amqp) = err.downcast_ref::<AmqpError>() {
                assert!(matches!(amqp, AmqpError::Connect(_)), "{amqp}");
                return;
            }
            cause = err.source();
        }
        panic!("startup did not fail with an AmqpError: {failed:?}");
    }
}
