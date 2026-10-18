//! The service of `examples/lapin_exchanges_topic.rs` without the macros: the handlers are named types bound to their descriptors, and `main` is
//! written by hand.
//!
//! ```text
//! cargo test --example manual_lapin_exchanges_topic --features testing
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_lapin::context::AmqpContext;
use serde::{Deserialize, Serialize};

use ruststream_lapin::prelude::*;

/// The slot order events leave through.
struct Events;

impl OutSlot for Events {
    const NAME: &'static str = "Events";
    // The publish call names the routing key, so a transform on this slot may name it too.
    type Destination = Names;
}

impl PublishedThrough<Events> for OrderEvent {}

/// Publishes each step of an order to the exchange `events`, as `order.<step>`.
struct Announce;

impl<Entry> Handle<Step, (), Outs<(Entry,)>> for Announce
where
    Entry: OutEntry<Events, Wire: Publisher>,
{
    async fn handle(
        &self,
        step: &Step,
        outs: &Outs<(Entry,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let event = OrderEvent { id: step.order_id };
        let key = format!("order.{}", step.name);
        if outs
            .get(Events)
            .message(&event)
            .to(&key)
            .publish()
            .await
            .is_err()
        {
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
}

// --8<-- [start:topic]
/// Every order event: `order.created`, `order.shipped` and the rest.
struct Audit;

impl Handle<OrderEvent, (), (), AmqpContext> for Audit {
    fn handle(
        &self,
        event: &OrderEvent,
        _outs: &(),
        ctx: &mut Context<'_, AmqpContext>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        let key = ctx.context(RoutingKey);
        tracing::info!(event.id, %key, "order event audited");
        ready(Ok(()))
    }
}

/// Only the shipments.
struct NotifyShipping;

impl Handle<OrderEvent> for NotifyShipping {
    fn handle(
        &self,
        event: &OrderEvent,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(event.id, "shipping notified");
        ready(Ok(()))
    }
}

fn app() -> impl App {
    let broker = LapinBroker::new("amqp://localhost:5672").declare_topology(true);
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        let events = RabbitExchange::topic("events");
        b.include(subscriber("order-steps", Announce).build())
            .out(Events, Publish::default().exchange("events"))
            .build();
        let audit = RabbitQueue::new("audit").bind(events.clone(), "order.*");
        b.include(subscriber(audit, Audit).build());
        let shipping = RabbitQueue::new("shipping").bind(events, "order.shipped");
        b.include(subscriber(shipping, NotifyShipping).build());
    })
}
// --8<-- [end:topic]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Step {
    order_id: u64,
    name: String,
}

// `#[outgoing(name = "order-steps")]` by hand: the type names the queue it is published to.
impl OutgoingDestination for Step {
    type Form = FixedName;
    const DESTINATION: &'static str = "order-steps";
}

impl MessageHeaders for Step {
    type Contract = NoHeaders;
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct OrderEvent {
    id: u64,
}

// `#[derive(Outgoing)]` by hand: the publish call names the destination.
impl OutgoingDestination for OrderEvent {
    type Form = CallerName;
}

impl MessageHeaders for OrderEvent {
    type Contract = NoHeaders;
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_lapin::prelude::*;

    use super::{OrderEvent, Step, app};

    #[tokio::test]
    async fn a_created_order_reaches_the_audit_alone() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<LapinBroker>()
            .message(&Step {
                order_id: 7,
                name: "created".to_owned(),
            })
            .publish()
            .await?;

        tb.broker::<LapinBroker>()
            .subscriber("audit")
            .assert_called_once()
            .with(&OrderEvent { id: 7 });
        tb.broker::<LapinBroker>()
            .subscriber("shipping")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_shipped_order_reaches_both_queues() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<LapinBroker>()
            .message(&Step {
                order_id: 8,
                name: "shipped".to_owned(),
            })
            .publish()
            .await?;

        tb.broker::<LapinBroker>()
            .subscriber("audit")
            .assert_called_once()
            .with(&OrderEvent { id: 8 });
        tb.broker::<LapinBroker>()
            .subscriber("shipping")
            .assert_called_once()
            .with(&OrderEvent { id: 8 });
        tb.shutdown().await?;
        Ok(())
    }

    // `*` matches exactly one word, so a key of three words passes `order.*` by.
    #[tokio::test]
    async fn a_key_of_three_words_passes_one_word_patterns_by() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<LapinBroker>()
            .message(&Step {
                order_id: 9,
                name: "shipped.late".to_owned(),
            })
            .publish()
            .await?;

        tb.broker::<LapinBroker>()
            .subscriber("audit")
            .assert_not_called();
        tb.broker::<LapinBroker>()
            .subscriber("shipping")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }
}
