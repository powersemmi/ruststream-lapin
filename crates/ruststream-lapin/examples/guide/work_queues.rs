//! The invoices worker of the Work queues guide: one queue shared by every instance, eight keyed
//! lanes per instance, a batch handler for the archive and a ledger read by one consumer at a time.
//!
//! ```text
//! docker compose -f docker-compose.test.yml up -d rabbitmq
//! cargo run --example guide_work_queues --features testing,ruststream/logging -- run
//! ```

use std::time::Duration;

use ruststream_lapin::prelude::*;
use serde::{Deserialize, Serialize};

// --8<-- [start:lanes]
#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
struct Invoice {
    id: u64,
    customer: String,
    total_cents: u64,
}

/// Renders one invoice; the invoices of one customer arrive in order on one lane.
#[subscriber(RabbitQueue::new("invoices"), workers(8, by_key))]
async fn render_invoice(invoice: &Invoice, Ctx(redelivered): Ctx<Redelivered>) -> HandlerOutcome {
    tracing::info!(
        invoice.id,
        invoice.customer,
        redelivered,
        "invoice rendered"
    );
    HandlerOutcome::ack()
}
// --8<-- [end:lanes]

// --8<-- [start:batches]
/// Writes rendered invoices to the archive in batches of up to 64.
#[subscriber(RabbitQueue::new("invoices.archive")
    .prefetch(nonzero!(64))
    .batch_wait(Duration::from_millis(200)))]
async fn archive(invoices: &[Invoice]) -> HandlerOutcome {
    tracing::info!(count = invoices.len(), "invoices archived");
    HandlerOutcome::ack()
}
// --8<-- [end:batches]

// --8<-- [start:single_active]
/// Posts invoices to the ledger, which needs them in queue order across every instance.
#[subscriber(RabbitQueue::new("invoices.ledger")
    .argument("x-single-active-consumer", AMQPValue::Boolean(true)))]
async fn post_to_ledger(invoice: &Invoice) -> HandlerOutcome {
    tracing::info!(invoice.id, invoice.total_cents, "invoice posted");
    HandlerOutcome::ack()
}
// --8<-- [end:single_active]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App {
    let broker = LapinBroker::new("amqp://localhost:5672")
        .prefetch(nonzero!(32))
        .declare_topology(true);
    RustStream::new(AppInfo::new("invoices", "0.1.0")).with_broker(broker, |b| {
        b.include(render_invoice);
        b.include(archive.batch(nonzero!(64)));
        b.include(post_to_ledger);
    })
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_lapin::PARTITION_KEY_HEADER;
    use ruststream_lapin::prelude::*;

    use super::{Invoice, app};

    fn invoice(id: u64, customer: &str) -> Invoice {
        Invoice {
            id,
            customer: customer.to_owned(),
            total_cents: 4_200,
        }
    }

    /// Publishes `invoice` to `queue` keyed by its customer, as the billing service does.
    async fn publish_keyed<S: Send + Sync + 'static>(
        tb: &TestApp<S>,
        queue: &str,
        invoice: &Invoice,
    ) -> Result<(), Box<dyn Error>> {
        let mut key = HeaderMap::new();
        key.insert(PARTITION_KEY_HEADER, invoice.customer.clone());
        tb.broker::<LapinBroker>()
            .message(invoice)
            .to(queue)
            .with_headers(key)
            .publish()
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn the_invoices_of_one_customer_are_rendered_in_order() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        for sent in [invoice(1, "acme"), invoice(2, "globex"), invoice(3, "acme")] {
            publish_keyed(&tb, "invoices", &sent).await?;
        }

        let rendered = tb
            .broker::<LapinBroker>()
            .subscriber("invoices")
            .assert_called(3)
            .settled(HandlerOutcome::ack())
            .received::<Invoice>();
        let acme: Vec<u64> = rendered
            .iter()
            .filter(|invoice| invoice.customer == "acme")
            .map(|invoice| invoice.id)
            .collect();
        assert_eq!(acme, [1, 3]);
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn the_archive_takes_invoices_in_batches() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        for id in 1..=2 {
            publish_keyed(&tb, "invoices.archive", &invoice(id, "acme")).await?;
        }

        // Each publish returns once its batch settled, so each invoice is a batch of its own here.
        tb.broker::<LapinBroker>()
            .subscriber("invoices.archive")
            .assert_batch_sizes(&[1, 1])
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn the_ledger_queue_takes_its_argument_and_posts() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        publish_keyed(&tb, "invoices.ledger", &invoice(9, "acme")).await?;

        tb.broker::<LapinBroker>()
            .subscriber("invoices.ledger")
            .assert_called_once()
            .with(&invoice(9, "acme"))
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }
}
