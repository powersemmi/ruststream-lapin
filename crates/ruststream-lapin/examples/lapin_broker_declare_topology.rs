//! A service that declares its own topology: the exchange, the queue and the binding its
//! descriptor states.
//!
//! ```text
//! cargo test --example lapin_broker_declare_topology --features testing
//! ```

use serde::{Deserialize, Serialize};

// --8<-- [start:app]
use ruststream_lapin::prelude::*;

#[subscriber(RabbitQueue::new("orders").bind(RabbitExchange::topic("events"), "order.*"))]
async fn place(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "order placed");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    // The service owns `events`, `orders` and the binding between them.
    let broker = LapinBroker::new("amqp://localhost:5672").declare_topology(true);
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(place);
    })
}
// --8<-- [end:app]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

#[subscriber(RabbitQueue::new("orders").durable(false))]
async fn place_transiently(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "order placed");
    HandlerOutcome::ack()
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_lapin::AmqpError;
    use ruststream_lapin::prelude::*;

    use super::{Order, app, place_transiently};

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
            b.include(place_transiently);
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
