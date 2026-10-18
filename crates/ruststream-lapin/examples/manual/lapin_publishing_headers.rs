//! The service of `examples/lapin_publishing_headers.rs` written without `#[subscriber]`: the
//! bodies implement `Handle`, and the publish takes the same header map.
//!
//! ```text
//! cargo test --example manual_lapin_publishing_headers --features testing
//! cargo run --example manual_lapin_publishing_headers -- run
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
    reference: String,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "invoices")]
struct Invoice {
    order_id: u64,
}

// --8<-- [start:headers]
/// The invoice carries the order's reference as its `message-id` property and the tenant in the
/// header table.
struct IssueInvoice;

impl<Egress> Handle<Order, (), Outs<(Egress,)>> for IssueInvoice
where
    Egress: OutEntry<DefaultSlot, Wire: Publisher>,
{
    async fn handle(
        &self,
        order: &Order,
        outs: &Outs<(Egress,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let mut headers = HeaderMap::new();
        headers.insert("message-id", order.reference.clone());
        headers.insert("tenant", "acme");
        let invoice = Invoice { order_id: order.id };
        let invoices = outs.get(DefaultSlot);
        if let Err(err) = invoices
            .message(&invoice)
            .with_headers(headers)
            .publish()
            .await
        {
            // A reference longer than a short string is refused, and a retry will not shorten it.
            tracing::error!(order.id, %err, "invoice refused");
            return Err(HandlerOutcome::drop());
        }
        Ok(())
    }
}

/// The billing side reads both where it reads every header.
struct Bill;

impl Handle<Invoice> for Bill {
    fn handle(
        &self,
        invoice: &Invoice,
        _outs: &(),
        ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        let reference = ctx.headers().get_str("message-id").unwrap_or_default();
        let tenant = ctx.headers().get_str("tenant").unwrap_or_default();
        tracing::info!(invoice.order_id, reference, tenant, "invoice billed");
        ready(Ok(()))
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(
        LapinBroker::new("amqp://localhost:5672"),
        |b| {
            b.include(subscriber(RabbitQueue::new("orders"), IssueInvoice).build())
                .out(DefaultSlot, Publish::default())
                .build();
            b.include(subscriber(RabbitQueue::new("invoices"), Bill).build());
        },
    )
}
// --8<-- [end:headers]

fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_lapin::prelude::*;

    use super::{Invoice, Order, app};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn both_headers_reach_the_consumer() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let order = Order {
            id: 7,
            reference: "ord-7".into(),
        };
        tb.broker::<LapinBroker>().message(&order).publish().await?;

        tb.broker::<LapinBroker>()
            .published::<Invoice>("invoices")
            .assert_called_once()
            .with(&Invoice { order_id: 7 })
            .with_header("message-id", "ord-7")
            .with_header("tenant", "acme");
        tb.broker::<LapinBroker>()
            .subscriber("invoices")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_property_longer_than_a_short_string_is_refused() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let order = Order {
            id: 8,
            reference: "r".repeat(300),
        };
        tb.broker::<LapinBroker>().message(&order).publish().await?;

        tb.broker::<LapinBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        tb.broker::<LapinBroker>()
            .subscriber("invoices")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }
}
