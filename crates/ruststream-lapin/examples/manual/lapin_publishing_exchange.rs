//! The service of `examples/lapin_publishing_exchange.rs` written without `#[subscriber]`: the
//! bodies implement `Handle`, and the slot takes the same policy.
//!
//! ```text
//! cargo test --example manual_lapin_publishing_exchange --features testing
//! cargo run --example manual_lapin_publishing_exchange -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_lapin::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "order.placed")]
struct OrderPlaced {
    id: u64,
}

// --8<-- [start:exchange]
/// Every accepted order is announced on the `events` exchange under `order.placed`.
struct Accept;

impl<Egress> Handle<Order, (), Outs<(Egress,)>> for Accept
where
    Egress: OutEntry<DefaultSlot, Wire: Publisher>,
{
    async fn handle(
        &self,
        order: &Order,
        outs: &Outs<(Egress,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let placed = OrderPlaced { id: order.id };
        let events = outs.get(DefaultSlot);
        if events.message(&placed).publish().await.is_err() {
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
}

/// The audit service's queue takes every `order.*` event from that exchange.
struct Audit;

impl Handle<OrderPlaced> for Audit {
    fn handle(
        &self,
        placed: &OrderPlaced,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(placed.id, "order placed");
        ready(Ok(()))
    }
}

fn app() -> impl App {
    let broker = LapinBroker::new("amqp://localhost:5672").declare_topology(true);
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        let audit =
            RabbitQueue::new("orders.audit").bind(RabbitExchange::topic("events"), "order.*");
        b.include(subscriber(RabbitQueue::new("orders"), Accept).build())
            .out(DefaultSlot, Publish::default().exchange("events"))
            .build();
        b.include(subscriber(audit, Audit).build());
    })
}
// --8<-- [end:exchange]

fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_lapin::prelude::*;

    use super::{Order, OrderPlaced, app};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_event_reaches_the_bound_queue() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<LapinBroker>()
            .message(&Order { id: 7 })
            .publish()
            .await?;

        tb.broker::<LapinBroker>()
            .subscriber("orders.audit")
            .assert_called_once()
            .with(&OrderPlaced { id: 7 });
        tb.shutdown().await?;
        Ok(())
    }
}
