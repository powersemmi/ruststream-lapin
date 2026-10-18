//! The service of `examples/lapin_broker_connection.rs` without the macros: the handler is a named
//! type and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_lapin_broker_connection --features testing
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use ruststream_lapin::prelude::*;

struct Place;

impl Handle<Order> for Place {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, "order placed");
        ready(Ok(()))
    }
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

fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker(), |b| {
        b.include(subscriber("orders", Place).build());
    })
}
// --8<-- [end:broker]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
}

// `#[outgoing(name = "orders")]` by hand: an order names the queue it is published to.
impl OutgoingDestination for Order {
    type Form = FixedName;
    const DESTINATION: &'static str = "orders";
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_lapin::AmqpError;
    use ruststream_lapin::prelude::*;

    use super::{Order, Place, app};

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
                b.include(subscriber("orders", Place).build());
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
