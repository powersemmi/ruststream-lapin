//! A direct exchange: each queue is bound under the routing key it takes, and a message goes to
//! the queues bound under its key exactly.
//!
//! ```text
//! cargo test --example lapin_exchanges_direct --features testing
//! ```

use serde::{Deserialize, Serialize};

use ruststream_lapin::prelude::*;

#[derive(OutSlot)]
#[publishes(Payment)]
struct Payments;

/// Checkout publishes each order's payment to the exchange `payments`, under the payment method
/// as the routing key.
#[subscriber("checkout")]
async fn checkout(order: &Order, Out(payments): Out<impl Publisher, Payments>) -> HandlerOutcome {
    let payment = Payment { order_id: order.id };
    if payments
        .message(&payment)
        .to(&order.method)
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

// --8<-- [start:direct]
#[subscriber(RabbitQueue::new("payments.card").bind(RabbitExchange::direct("payments"), "card"))]
async fn charge_card(payment: &Payment) -> HandlerOutcome {
    tracing::info!(payment.order_id, "card charged");
    HandlerOutcome::ack()
}

#[subscriber(RabbitQueue::new("payments.invoice")
    .bind(RabbitExchange::direct("payments"), "invoice"))]
async fn send_invoice(payment: &Payment) -> HandlerOutcome {
    tracing::info!(payment.order_id, "invoice sent");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    let broker = LapinBroker::new("amqp://localhost:5672").declare_topology(true);
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(broker, |b| {
        b.include(checkout)
            .out(Payments, Publish::default().exchange("payments"))
            .build();
        b.include(charge_card);
        b.include(send_invoice);
    })
}
// --8<-- [end:direct]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "checkout")]
struct Order {
    id: u64,
    method: String,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Payment {
    order_id: u64,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_lapin::prelude::*;

    use super::{Order, Payment, app};

    #[tokio::test]
    async fn a_card_payment_reaches_the_card_queue_alone() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<LapinBroker>()
            .message(&Order {
                id: 7,
                method: "card".to_owned(),
            })
            .publish()
            .await?;

        tb.broker::<LapinBroker>()
            .subscriber("payments.card")
            .assert_called_once()
            .with(&Payment { order_id: 7 });
        tb.broker::<LapinBroker>()
            .subscriber("payments.invoice")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_key_no_queue_is_bound_under_reaches_no_queue() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<LapinBroker>()
            .message(&Order {
                id: 8,
                method: "cash".to_owned(),
            })
            .publish()
            .await?;

        tb.broker::<LapinBroker>()
            .subscriber("checkout")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.broker::<LapinBroker>()
            .subscriber("payments.card")
            .assert_not_called();
        tb.broker::<LapinBroker>()
            .subscriber("payments.invoice")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }
}
