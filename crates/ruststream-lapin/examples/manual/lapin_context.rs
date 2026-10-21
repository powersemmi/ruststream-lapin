//! The service of `examples/lapin_context.rs` without the macros: the handler is a named type over the delivery's context, and `main` is
//! written by hand.
//!
//! ```text
//! cargo test --example manual_lapin_context --features testing
//! ```

use std::future::ready;
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_lapin::context::AmqpContext;
use serde::{Deserialize, Serialize};

// --8<-- [start:handler]
use ruststream_lapin::prelude::*;

/// Records where each order came from: its exchange and routing key, and whether the server had
/// handed it out before.
struct Audit;

impl Handle<Order, AuditRecord, (), AmqpContext> for Audit {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        ctx: &mut Context<'_, AmqpContext>,
    ) -> impl Future<Output = Result<AuditRecord, HandlerOutcome>> {
        let exchange = ctx.context(Exchange);
        let routing_key = ctx.context(RoutingKey);
        let redelivered = ctx.context(Redelivered);
        let delivery_tag = ctx.context(DeliveryTag);
        tracing::info!(order.id, %exchange, %routing_key, redelivered, delivery_tag, "order audited");
        ready(Ok(AuditRecord {
            order_id: order.id,
            exchange: exchange.to_owned(),
            routing_key: routing_key.to_owned(),
            redelivered,
        }))
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        LapinBroker::new("amqp://localhost:5672"),
        |b| {
            let orders = RabbitQueue::new("orders");
            b.include(subscriber(orders, Audit).reply().build())
                .out_reply(Publish::default());
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
}

// `#[outgoing(name = "orders")]` by hand: the type names the queue it is published to.
impl OutgoingDestination for Order {
    type Form = FixedName;
    const DESTINATION: &'static str = "orders";
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct AuditRecord {
    order_id: u64,
    exchange: String,
    routing_key: String,
    redelivered: bool,
}

// `#[outgoing(name = "orders.audit")]` by hand: the type names the queue it is published to.
impl OutgoingDestination for AuditRecord {
    type Form = FixedName;
    const DESTINATION: &'static str = "orders.audit";
}

impl MessageHeaders for AuditRecord {
    type Contract = NoHeaders;
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_lapin::prelude::*;

    use super::{AuditRecord, Order, app};

    // The harness publishes on the default exchange, whose name is empty, under the queue's name.
    #[tokio::test]
    async fn the_record_names_the_default_exchange_and_the_queue() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<LapinBroker>()
            .message(&Order { id: 7 })
            .publish()
            .await?;

        tb.broker::<LapinBroker>()
            .published::<AuditRecord>("orders.audit")
            .assert_called_once()
            .with(&AuditRecord {
                order_id: 7,
                exchange: String::new(),
                routing_key: "orders".to_owned(),
                redelivered: false,
            });
        tb.shutdown().await?;
        Ok(())
    }
}
