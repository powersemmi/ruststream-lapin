//! The service of `examples/lapin_transactions_owned.rs` written without `#[subscriber]`: the
//! body implements `Handle` over a slot whose publisher opens owned transactions.
//!
//! ```text
//! cargo test --example manual_lapin_transactions_owned --features testing
//! cargo run --example manual_lapin_transactions_owned -- run
//! ```

use std::process::ExitCode;

use ruststream::OutSlot;
use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_lapin::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
    in_stock: bool,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "shipments")]
struct Shipment {
    order_id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "audit")]
struct AuditRecord {
    order_id: u64,
    shipped: bool,
}

// --8<-- [start:owned]
#[derive(OutSlot)]
#[publishes(Shipment, AuditRecord)]
struct Records;

/// The shipment and the audit record go out in two transactions. The shipment waits in its own
/// buffer while the stock is checked: an order out of stock aborts it and still commits its audit
/// record.
struct Record;

impl<Egress> Handle<Order, (), Outs<(Egress,)>> for Record
where
    Egress: OutEntry<Records, Wire: OwnedTransactions>,
{
    async fn handle(
        &self,
        order: &Order,
        outs: &Outs<(Egress,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let records = outs.get(Records);
        let (Ok(mut shipment), Ok(mut audit)) =
            (records.transaction().await, records.transaction().await)
        else {
            return Err(HandlerOutcome::retry());
        };
        let shipped = Shipment { order_id: order.id };
        if shipment.message(&shipped).publish().await.is_err() {
            return Err(HandlerOutcome::retry());
        }
        let settled = if order.in_stock {
            shipment.commit().await
        } else {
            shipment.abort().await
        };
        if settled.is_err() {
            return Err(HandlerOutcome::retry());
        }
        let entry = AuditRecord {
            order_id: order.id,
            shipped: order.in_stock,
        };
        if audit.message(&entry).publish().await.is_err() || audit.commit().await.is_err() {
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        LapinBroker::new("amqp://localhost:5672"),
        |b| {
            b.include(subscriber(RabbitQueue::new("orders"), Record).build())
                .out(Records, TransactionalPublish::default())
                .build();
        },
    )
}
// --8<-- [end:owned]

fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_lapin::prelude::*;

    use super::{AuditRecord, Order, Shipment, app};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_order_in_stock_commits_both() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let order = Order {
            id: 1,
            in_stock: true,
        };
        tb.broker::<LapinBroker>().message(&order).publish().await?;

        tb.broker::<LapinBroker>()
            .published::<Shipment>("shipments")
            .assert_called_once()
            .with(&Shipment { order_id: 1 });
        tb.broker::<LapinBroker>()
            .published::<AuditRecord>("audit")
            .assert_called_once()
            .with(&AuditRecord {
                order_id: 1,
                shipped: true,
            });
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_aborted_shipment_leaves_the_audit_committed() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let order = Order {
            id: 2,
            in_stock: false,
        };
        tb.broker::<LapinBroker>().message(&order).publish().await?;

        tb.broker::<LapinBroker>()
            .published::<Shipment>("shipments")
            .assert_not_called();
        tb.broker::<LapinBroker>()
            .published::<AuditRecord>("audit")
            .assert_called_once()
            .with(&AuditRecord {
                order_id: 2,
                shipped: false,
            });
        tb.shutdown().await?;
        Ok(())
    }
}
