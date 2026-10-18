//! The service of `examples/lapin_request_request.rs` written without `#[subscriber]`: the bodies
//! implement `Handle`, and the slot takes the same request policy.
//!
//! ```text
//! cargo test --example manual_lapin_request_request --features testing
//! cargo run --example manual_lapin_request_request -- run
//! ```

use std::error::Error;
use std::future::{Future, ready};
use std::process::ExitCode;
use std::time::Duration;

use ruststream::OutgoingMessage;
use ruststream::codec::{Codec, JsonCodec};
use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_lapin::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
    sku: String,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct CheckStock {
    sku: String,
}

#[derive(Debug, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Stock {
    available: bool,
}

// --8<-- [start:request]
/// Asks the inventory service whether `sku` is in stock, and waits two seconds at most.
async fn in_stock(
    inventory: &impl RequestReply,
    sku: &str,
) -> Result<bool, Box<dyn Error + Send + Sync>> {
    let request = JsonCodec.encode(&CheckStock {
        sku: sku.to_owned(),
    })?;
    let ask = OutgoingMessage::new("inventory.check", request.as_ref());
    let reply = inventory.request(ask, Duration::from_secs(2)).await?;
    let stock: Stock = JsonCodec.decode(reply.payload())?;
    Ok(stock.available)
}

/// An order in stock is accepted and one out of stock is dropped. An inventory service that does
/// not answer brings the order back.
struct PlaceOrder;

impl<Egress> Handle<Order, (), Outs<(Egress,)>> for PlaceOrder
where
    Egress: OutEntry<DefaultSlot, Wire: RequestReply>,
{
    async fn handle(
        &self,
        order: &Order,
        outs: &Outs<(Egress,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        match in_stock(outs.get(DefaultSlot), &order.sku).await {
            Ok(true) => Ok(()),
            Ok(false) => Err(HandlerOutcome::drop()),
            Err(err) => {
                tracing::warn!(order.id, %err, "inventory unavailable");
                Err(HandlerOutcome::retry())
            }
        }
    }
}
// --8<-- [end:request]

/// The inventory service, mounted in the same app for the example.
struct CheckStockBody;

impl Handle<CheckStock, Stock> for CheckStockBody {
    fn handle(
        &self,
        ask: &CheckStock,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Stock, HandlerOutcome>> {
        ready(Ok(Stock {
            available: ask.sku != "unobtainium",
        }))
    }
}

// --8<-- [start:app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        LapinBroker::new("amqp://localhost:5672"),
        |b| {
            let check = subscriber(RabbitQueue::new("inventory.check"), CheckStockBody);
            b.include(subscriber(RabbitQueue::new("orders"), PlaceOrder).build())
                .out(DefaultSlot, Request::default())
                .build();
            b.include(check.reply().to("inventory.check.unrouted").build())
                .out_reply(Publish::default())
                .transform(DirectReplyTo);
        },
    )
}
// --8<-- [end:app]

fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_lapin::prelude::*;

    use super::{Order, app};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_reply_decides_the_order() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let order = Order {
            id: 1,
            sku: "widget".into(),
        };
        tb.broker::<LapinBroker>().message(&order).publish().await?;

        tb.broker::<LapinBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.broker::<LapinBroker>()
            .subscriber("inventory.check")
            .assert_called_once();
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_order_out_of_stock_is_dropped() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let order = Order {
            id: 2,
            sku: "unobtainium".into(),
        };
        tb.broker::<LapinBroker>().message(&order).publish().await?;

        tb.broker::<LapinBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        tb.shutdown().await?;
        Ok(())
    }
}
