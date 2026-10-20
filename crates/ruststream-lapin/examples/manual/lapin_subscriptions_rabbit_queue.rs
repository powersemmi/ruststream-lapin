//! The service of `examples/lapin_subscriptions_rabbit_queue.rs` without the macros: the handlers are named types bound to their descriptors, and `main` is
//! written by hand.
//!
//! ```text
//! cargo test --example manual_lapin_subscriptions_rabbit_queue --features testing
//! ```

use std::future::ready;
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_lapin::context::AmqpContext;
use serde::{Deserialize, Serialize};

use ruststream_lapin::prelude::*;

// --8<-- [start:descriptor]
/// A delivery the server already handed out once is placed on its second try.
struct Place;

impl Handle<Order, (), (), AmqpContext> for Place {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        ctx: &mut Context<'_, AmqpContext>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        let redelivered = ctx.context(Redelivered);
        if !redelivered && order.flaky {
            return ready(Err(HandlerOutcome::retry()));
        }
        tracing::info!(order.id, redelivered, "order placed");
        ready(Ok(()))
    }
}

/// A durable queue fed by the topic exchange `events`; a dropped order leaves through
/// `orders.failed`.
fn orders() -> RabbitQueue {
    RabbitQueue::new("orders")
        .bind(RabbitExchange::topic("events"), "order.*")
        .dead_letter_exchange("orders.failed")
}

struct Refresh;

impl Handle<Price> for Refresh {
    fn handle(
        &self,
        price: &Price,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(price.sku, price.cents, "price cached");
        ready(Ok(()))
    }
}

/// A queue of this instance alone: it lives as long as the connection that declared it.
fn prices() -> RabbitQueue {
    RabbitQueue::new("prices.instance-a")
        .durable(false)
        .exclusive(true)
        .auto_delete(true)
        .bind(RabbitExchange::fanout("prices"), "")
}
// --8<-- [end:descriptor]

fn app() -> impl App {
    let broker = LapinBroker::new("amqp://localhost:5672").declare_topology(true);
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(subscriber(orders(), Place).build());
        b.include(subscriber(prices(), Refresh).build());
    })
}

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
    flaky: bool,
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
struct Price {
    sku: String,
    cents: u64,
}

// `#[outgoing(name = "prices.instance-a")]` by hand: the type names the queue it is published to.
impl OutgoingDestination for Price {
    type Form = FixedName;
    const DESTINATION: &'static str = "prices.instance-a";
}

impl MessageHeaders for Price {
    type Contract = NoHeaders;
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_lapin::prelude::*;

    use super::{Order, Price, app};

    #[tokio::test]
    async fn a_requeued_order_comes_back_redelivered() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<LapinBroker>()
            .message(&Order { id: 7, flaky: true })
            .publish()
            .await?;

        tb.broker::<LapinBroker>()
            .subscriber("orders")
            .assert_called(2)
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn the_exclusive_queue_takes_a_price() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<LapinBroker>()
            .message(&Price {
                sku: "tea".to_owned(),
                cents: 450,
            })
            .publish()
            .await?;

        tb.broker::<LapinBroker>()
            .subscriber("prices.instance-a")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }
}
