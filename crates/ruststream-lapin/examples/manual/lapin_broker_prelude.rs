//! The service of `examples/lapin_broker_prelude.rs` without the macros: the handler is a named
//! type over the delivery's context, and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_lapin_broker_prelude --features testing
//! ```

use std::future::ready;
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_lapin::context::AmqpContext;
use serde::{Deserialize, Serialize};

// --8<-- [start:app]
use ruststream_lapin::prelude::*;

struct Confirm;

impl Handle<Order, Confirmation, (), AmqpContext> for Confirm {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        ctx: &mut Context<'_, AmqpContext>,
    ) -> impl Future<Output = Result<Confirmation, HandlerOutcome>> {
        let routing_key = ctx.context(RoutingKey);
        tracing::info!(order.id, %routing_key, "order confirmed");
        ready(Ok(Confirmation { id: order.id }))
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        LapinBroker::new("amqp://localhost:5672"),
        |b| {
            let orders =
                RabbitQueue::new("orders").bind(RabbitExchange::topic("events"), "order.*");
            b.include(subscriber(orders, Confirm).reply().build())
                .out_reply(TransactionalPublish::default());
        },
    )
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

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Confirmation {
    id: u64,
}

// `#[outgoing(name = "confirmations")]` by hand: the reply names the queue it goes to.
impl OutgoingDestination for Confirmation {
    type Form = FixedName;
    const DESTINATION: &'static str = "confirmations";
}

impl MessageHeaders for Confirmation {
    type Contract = NoHeaders;
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_lapin::prelude::*;

    use super::{Confirmation, Order, app};

    #[tokio::test]
    async fn an_order_is_confirmed_on_the_reply_queue() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<LapinBroker>()
            .message(&Order { id: 7 })
            .publish()
            .await?;

        tb.broker::<LapinBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.broker::<LapinBroker>()
            .published::<Confirmation>("confirmations")
            .assert_called_once()
            .with(&Confirmation { id: 7 });
        tb.shutdown().await?;
        Ok(())
    }
}
