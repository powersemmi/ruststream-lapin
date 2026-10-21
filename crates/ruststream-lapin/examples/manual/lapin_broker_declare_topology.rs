//! The service of `examples/lapin_broker_declare_topology.rs` without the macros: the handler is a
//! named type bound to its descriptor, and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_lapin_broker_declare_topology --features testing
//! ```

use std::future::ready;
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// --8<-- [start:app]
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

fn app() -> impl App {
    // The service owns `events`, `orders` and the binding between them.
    let broker = LapinBroker::new("amqp://localhost:5672").declare_topology(true);
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        let orders = RabbitQueue::new("orders").bind(RabbitExchange::topic("events"), "order.*");
        b.include(subscriber(orders, Place).build());
    })
}
// --8<-- [end:app]

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
    async fn the_declared_queue_takes_an_order() -> Result<(), Box<dyn Error>> {
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

    // RabbitMQ 4 refuses a transient queue that is not exclusive, and so does the in-process
    // transport.
    #[tokio::test]
    async fn a_transient_shared_queue_fails_startup() {
        let broker = LapinBroker::new("amqp://localhost:5672").declare_topology(true);
        let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
            let transient = RabbitQueue::new("orders").durable(false);
            b.include(subscriber(transient, Place).build());
        });

        let failed = TestApp::start(app).await.err();
        let mut cause: Option<&(dyn Error + 'static)> =
            failed.as_ref().map(|err| -> &(dyn Error + 'static) { err });
        while let Some(err) = cause {
            if let Some(amqp) = err.downcast_ref::<AmqpError>() {
                assert!(matches!(amqp, AmqpError::Declare(_)), "{amqp}");
                return;
            }
            cause = err.source();
        }
        panic!("startup did not fail with an AmqpError: {failed:?}");
    }
}
