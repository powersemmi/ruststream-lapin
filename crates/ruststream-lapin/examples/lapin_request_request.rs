//! A handler that calls another service through the broker and waits for its answer: the request
//! goes out over direct reply-to, and the correlated reply resolves the call.
//!
//! ```text
//! cargo test --example lapin_request_request --features testing
//! cargo run --example lapin_request_request -- run
//! ```

use std::error::Error;
use std::time::Duration;

use ruststream::OutgoingMessage;
use ruststream::codec::{Codec, JsonCodec};
use ruststream_lapin::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
    sku: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct CheckStock {
    sku: String,
}

#[derive(Debug, Deserialize, Serialize, Outgoing)]
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
#[subscriber(RabbitQueue::new("orders"))]
async fn place_order(order: &Order, Out(inventory): Out<impl RequestReply>) -> HandlerOutcome {
    match in_stock(inventory, &order.sku).await {
        Ok(true) => HandlerOutcome::ack(),
        Ok(false) => HandlerOutcome::drop(),
        Err(err) => {
            tracing::warn!(order.id, %err, "inventory unavailable");
            HandlerOutcome::retry()
        }
    }
}
// --8<-- [end:request]

/// The inventory service, mounted in the same app for the example.
#[subscriber(RabbitQueue::new("inventory.check"), reply("inventory.check.unrouted"))]
async fn check_stock(ask: &CheckStock) -> Stock {
    Stock {
        available: ask.sku != "unobtainium",
    }
}

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        LapinBroker::new("amqp://localhost:5672"),
        |b| {
            b.include(place_order)
                .out(DefaultSlot, Request::default())
                .build();
            b.include(check_stock)
                .out_reply(Publish::default())
                .transform(DirectReplyTo);
        },
    )
}
// --8<-- [end:app]

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
