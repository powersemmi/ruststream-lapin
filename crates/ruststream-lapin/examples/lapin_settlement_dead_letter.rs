//! A queue with a dead-letter exchange: every message rejected without requeue leaves for that
//! exchange, and a queue bound to it keeps it for an operator.
//!
//! ```text
//! cargo test --example lapin_settlement_dead_letter --features testing
//! cargo run --example lapin_settlement_dead_letter -- run
//! ```

use ruststream_lapin::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "invoices")]
struct Invoice {
    id: u64,
    amount_cents: i64,
}

// --8<-- [start:dlx]
/// An invoice with a negative amount is rejected for good. The queue dead-letters it to
/// `invoices.dlx` under the routing key `invoices.rejected`.
#[subscriber(RabbitQueue::new("invoices")
    .dead_letter_exchange("invoices.dlx")
    .dead_letter_routing_key("invoices.rejected"))]
async fn book(invoice: &Invoice) -> HandlerOutcome {
    if invoice.amount_cents < 0 {
        tracing::warn!(invoice.id, "negative amount, rejected");
        return HandlerOutcome::drop();
    }
    HandlerOutcome::ack()
}

/// The rejected invoices wait here, bound to the dead-letter exchange under that key.
#[subscriber(RabbitQueue::new("invoices.rejected")
    .bind(RabbitExchange::direct("invoices.dlx"), "invoices.rejected"))]
async fn review(invoice: &Invoice) -> HandlerOutcome {
    tracing::error!(invoice.id, "invoice needs a review");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    let broker = LapinBroker::new("amqp://localhost:5672").declare_topology(true);
    RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(broker, |b| {
        b.include(book);
        b.include(review);
    })
}
// --8<-- [end:dlx]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_lapin::prelude::*;

    use super::{Invoice, app};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_rejected_invoice_reaches_the_dead_letter_queue() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let invoice = Invoice {
            id: 3,
            amount_cents: -100,
        };
        tb.broker::<LapinBroker>()
            .message(&invoice)
            .publish()
            .await?;

        tb.broker::<LapinBroker>()
            .subscriber("invoices")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        tb.broker::<LapinBroker>()
            .subscriber("invoices.rejected")
            .assert_called_once()
            .with(&invoice);
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_acked_invoice_stays_out_of_it() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let invoice = Invoice {
            id: 4,
            amount_cents: 100,
        };
        tb.broker::<LapinBroker>()
            .message(&invoice)
            .publish()
            .await?;

        tb.broker::<LapinBroker>()
            .subscriber("invoices.rejected")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }
}
