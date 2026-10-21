//! The service of `examples/lapin_subscriptions_queue.rs` without the macros: the handler is a named type, and `main` is
//! written by hand.
//!
//! ```text
//! cargo test --example manual_lapin_subscriptions_queue --features testing
//! ```

use std::future::ready;
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;

use serde::{Deserialize, Serialize};

// --8<-- [start:handler]
use ruststream_lapin::prelude::*;

struct Place;

impl Handle<Order> for Place {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        if order.items == 0 {
            tracing::warn!(order.id, "empty order dropped");
            return ready(Err(HandlerOutcome::drop()));
        }
        tracing::info!(order.id, order.items, "order placed");
        ready(Ok(()))
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        LapinBroker::new("amqp://localhost:5672"),
        |b| {
            b.include(subscriber("orders", Place).build());
        },
    )
}
// --8<-- [end:handler]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
    items: u32,
}

// `#[outgoing(name = "orders")]` by hand: the type names the queue it is published to.
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
    use ruststream_lapin::prelude::*;

    use super::{Order, app};

    #[tokio::test]
    async fn an_order_with_items_is_acked() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<LapinBroker>()
            .message(&Order { id: 7, items: 2 })
            .publish()
            .await?;

        tb.broker::<LapinBroker>()
            .subscriber("orders")
            .assert_called_once()
            .with(&Order { id: 7, items: 2 })
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn an_empty_order_is_dropped() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<LapinBroker>()
            .message(&Order { id: 8, items: 0 })
            .publish()
            .await?;

        tb.broker::<LapinBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        tb.shutdown().await?;
        Ok(())
    }
}
