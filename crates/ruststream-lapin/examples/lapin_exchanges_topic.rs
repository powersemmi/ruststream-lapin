//! A topic exchange: queues bind with dotted patterns, where `*` matches one word and `#` any
//! number of words.
//!
//! ```text
//! cargo test --example lapin_exchanges_topic --features testing
//! ```

use serde::{Deserialize, Serialize};

use ruststream_lapin::prelude::*;

#[derive(OutSlot)]
#[publishes(OrderEvent)]
struct Events;

/// Publishes each step of an order to the exchange `events`, as `order.<step>`.
#[subscriber("order-steps")]
async fn announce(step: &Step, Out(events): Out<impl Publisher, Events>) -> HandlerOutcome {
    let event = OrderEvent { id: step.order_id };
    let key = format!("order.{}", step.name);
    if events.message(&event).to(&key).publish().await.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

// --8<-- [start:topic]
/// Every order event: `order.created`, `order.shipped` and the rest.
#[subscriber(RabbitQueue::new("audit").bind(RabbitExchange::topic("events"), "order.*"))]
async fn audit(event: &OrderEvent, Ctx(key): Ctx<RoutingKey>) -> HandlerOutcome {
    tracing::info!(event.id, %key, "order event audited");
    HandlerOutcome::ack()
}

/// Only the shipments.
#[subscriber(RabbitQueue::new("shipping")
    .bind(RabbitExchange::topic("events"), "order.shipped"))]
async fn notify_shipping(event: &OrderEvent) -> HandlerOutcome {
    tracing::info!(event.id, "shipping notified");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    let broker = LapinBroker::new("amqp://localhost:5672").declare_topology(true);
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(announce)
            .out(Events, Publish::default().exchange("events"))
            .build();
        b.include(audit);
        b.include(notify_shipping);
    })
}
// --8<-- [end:topic]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "order-steps")]
struct Step {
    order_id: u64,
    name: String,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct OrderEvent {
    id: u64,
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
