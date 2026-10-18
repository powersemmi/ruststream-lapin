//! The confirms policy outside a transaction: each publish resolves once the broker confirmed the
//! message, so a payment is acknowledged only after its receipt is the broker's.
//!
//! ```text
//! cargo test --example lapin_transactions_confirms --features testing
//! AMQP_TEST_URL=amqp://127.0.0.1:5672 RUSTSTREAM_REQUIRE_LIVE=1 \
//!     cargo test --example lapin_transactions_confirms --features testing
//! ```

use ruststream_lapin::prelude::*;
use serde::{Deserialize, Serialize};

#[cfg(test)]
#[path = "../tests/live/mod.rs"]
mod live;

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Payment {
    id: u64,
    cents: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "payments.receipts")]
struct Receipt {
    payment_id: u64,
}

// --8<-- [start:confirms]
/// The payment is acknowledged once the broker has confirmed its receipt. A receipt the broker
/// refused brings the payment back.
#[subscriber(RabbitQueue::new("payments"))]
async fn settle(payment: &Payment, Out(receipts): Out<impl Publisher>) -> HandlerOutcome {
    let receipt = Receipt {
        payment_id: payment.id,
    };
    if let Err(err) = receipts.message(&receipt).publish().await {
        tracing::warn!(payment.id, %err, "receipt not confirmed");
        return HandlerOutcome::retry();
    }
    tracing::info!(payment.id, payment.cents, "payment settled");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    let broker = LapinBroker::new("amqp://localhost:5672").declare_topology(true);
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(broker, |b| {
        b.include(settle)
            .out(DefaultSlot, TransactionalPublish::default())
            .build();
    })
}
// --8<-- [end:confirms]

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::process;

    use ruststream::testing::TestApp;
    use ruststream_lapin::prelude::*;

    use super::{Payment, Receipt, app, live, settle};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_confirmed_receipt_settles_the_payment() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<LapinBroker>()
            .message(&Payment { id: 1, cents: 990 })
            .to("payments")
            .publish()
            .await?;

        tb.broker::<LapinBroker>()
            .published::<Receipt>("payments.receipts")
            .assert_called_once()
            .with(&Receipt { payment_id: 1 });
        tb.broker::<LapinBroker>()
            .subscriber("payments")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    // No queue is bound under the receipt's routing key on the server: RabbitMQ confirms the
    // publish and drops the message, so the handler's publish succeeds and the payment is acked.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_server_confirms_a_receipt_no_queue_takes() -> Result<(), Box<dyn Error>> {
        let Some(url) = live::url("AMQP_TEST_URL") else {
            return Ok(());
        };
        let queue = format!("payments-confirms-{}", process::id());
        let source = RabbitQueue::new(queue.clone()).auto_delete(true);
        let broker = LapinBroker::new(url).declare_topology(true);
        let app = RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(broker, |b| {
            b.include(settle.map_source(|_| source))
                .out(DefaultSlot, TransactionalPublish::default())
                .build();
        });
        let tb = TestApp::start_live(app).await?;
        tb.broker::<LapinBroker>()
            .message(&Payment { id: 2, cents: 120 })
            .to(&queue)
            .publish()
            .await?;

        tb.broker::<LapinBroker>()
            .subscriber(&queue)
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }
}
