//! The service of `examples/lapin_settlement_outcomes.rs` written without `#[subscriber]`: the
//! body implements `Handle` over this crate's `AmqpContext` and reads the redelivered flag there.
//!
//! ```text
//! cargo test --example manual_lapin_settlement_outcomes --features testing
//! cargo run --example manual_lapin_settlement_outcomes -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_lapin::context::AmqpContext;
use ruststream_lapin::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
    sku: String,
}

// --8<-- [start:outcomes]
/// An order without a SKU is rejected for good. Any other order is requeued once, the first time
/// it arrives, and acknowledged when the queue hands it out again marked redelivered.
struct Reserve;

impl Handle<Order, (), (), AmqpContext> for Reserve {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        ctx: &mut Context<'_, AmqpContext>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        let outcome = if order.sku.is_empty() {
            tracing::warn!(order.id, "order without a SKU, rejected");
            Err(HandlerOutcome::drop())
        } else if !ctx.context(Redelivered) {
            tracing::info!(order.id, "stock not counted yet, back to the queue");
            Err(HandlerOutcome::retry())
        } else {
            tracing::info!(order.id, "reserved");
            Ok(())
        };
        ready(outcome)
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("stock", "0.1.0")).with_broker(
        LapinBroker::new("amqp://localhost:5672"),
        |b| {
            b.include(subscriber(RabbitQueue::new("orders"), Reserve).build());
        },
    )
}
// --8<-- [end:outcomes]

fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::{Outcome, TestApp};
    use ruststream_lapin::prelude::*;

    use super::{Order, app};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_retry_comes_back_redelivered_and_is_acked() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let order = Order {
            id: 7,
            sku: "widget".into(),
        };
        tb.broker::<LapinBroker>().message(&order).publish().await?;

        assert_eq!(
            tb.broker::<LapinBroker>().subscriber("orders").outcomes(),
            [Outcome::Nack, Outcome::Ack],
        );
        tb.broker::<LapinBroker>()
            .subscriber("orders")
            .assert_called(2)
            .with(&order);
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_drop_does_not_come_back() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let order = Order {
            id: 8,
            sku: String::new(),
        };
        tb.broker::<LapinBroker>().message(&order).publish().await?;

        tb.broker::<LapinBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        tb.shutdown().await?;
        Ok(())
    }
}
