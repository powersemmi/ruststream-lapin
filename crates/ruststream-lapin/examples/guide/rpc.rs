//! The orders service of the Request and reply guide asks the inventory service for stock over
//! `RabbitMQ` direct reply-to before it accepts an order. Both sides are routers: each service
//! mounts its own, and this example runs both in one process.
//!
//! ```text
//! docker compose -f docker-compose.test.yml up -d rabbitmq
//! cargo run --example guide_rpc --features testing,ruststream/logging -- run
//! ```

use std::error::Error;
use std::time::Duration;

use ruststream::OutgoingMessage;
use ruststream::codec::{Codec, JsonCodec};
use ruststream_lapin::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct CheckStock {
    sku: String,
    quantity: u32,
}

// --8<-- [start:responder]
/// The stock of one SKU. An RPC reply goes where the request asked, so the type names no
/// destination.
#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
struct Stock {
    sku: String,
    available: bool,
}

#[subscriber("inventory.check", reply("inventory.check.unanswered"))]
async fn check_stock(request: &CheckStock) -> Stock {
    tracing::info!(request.sku, request.quantity, "stock checked");
    Stock {
        sku: request.sku.clone(),
        available: request.quantity <= 10,
    }
}

/// The inventory service's routes: each reply goes to the requester's own address.
fn inventory() -> Router<LapinBroker, impl RouterDef<LapinBroker>> {
    Router::new()
        .include(check_stock)
        .out_reply(Publish::default())
        .transform(DirectReplyTo)
        .build()
}
// --8<-- [end:responder]

// --8<-- [start:requester]
/// How long an order waits for the inventory service.
const RPC_TIMEOUT: Duration = Duration::from_secs(2);
/// How long an order waits before it asks again when the inventory service did not answer.
const RETRY: Duration = Duration::from_secs(30);

#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
struct Order {
    id: u64,
    sku: String,
    quantity: u32,
}

/// Asks the inventory service and waits for its answer.
async fn ask_stock(
    inventory: &impl RequestReply,
    order: &Order,
) -> Result<Stock, Box<dyn Error + Send + Sync>> {
    let request = JsonCodec.encode(&CheckStock {
        sku: order.sku.clone(),
        quantity: order.quantity,
    })?;
    let reply = inventory
        .request(
            OutgoingMessage::new("inventory.check", request.as_ref()),
            RPC_TIMEOUT,
        )
        .await?;
    Ok(JsonCodec.decode(reply.payload())?)
}

/// A delayed order waits in the broker's waiting queue, `orders.retry`.
#[subscriber(RabbitQueue::new("orders").delay(Delay::dlx_ttl()))]
async fn place_order(order: &Order, Out(inventory): Out<impl RequestReply>) -> HandlerOutcome {
    match ask_stock(inventory, order).await {
        Ok(stock) if stock.available => {
            tracing::info!(order.id, "order accepted");
            HandlerOutcome::ack()
        }
        Ok(_) => {
            tracing::warn!(order.id, "order rejected, out of stock");
            HandlerOutcome::drop()
        }
        Err(err) => {
            tracing::warn!(order.id, %err, "inventory unavailable, asking again later");
            HandlerOutcome::retry_after(RETRY)
        }
    }
}

/// The orders service's routes: the requester is a policy, paired with the connection at startup.
fn orders() -> Router<LapinBroker, impl RouterDef<LapinBroker>> {
    Router::new()
        .include(place_order)
        .out(DefaultSlot, Request::default())
        .build()
}

#[ruststream::app]
fn app() -> impl App {
    let broker = LapinBroker::new("amqp://localhost:5672").declare_topology(true);
    // The example runs both services in one process; each service mounts its own router.
    RustStream::new(AppInfo::new("shop", "0.1.0")).with_broker(broker, |b| {
        b.include_router(orders());
        b.include_router(inventory());
    })
}
// --8<-- [end:requester]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_lapin::prelude::*;

    use super::{Order, RETRY, app, orders};

    /// The orders service alone, as it is deployed: nobody answers on `inventory.check`.
    fn orders_alone() -> RustStream {
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
            LapinBroker::new("amqp://localhost:5672").declare_topology(true),
            |b| {
                b.include_router(orders());
            },
        )
    }

    fn order(id: u64, quantity: u32) -> Order {
        Order {
            id,
            sku: "kettle".to_owned(),
            quantity,
        }
    }

    #[tokio::test]
    async fn an_order_in_stock_is_accepted() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<LapinBroker>()
            .message(&order(1, 2))
            .to("orders")
            .publish()
            .await?;

        tb.broker::<LapinBroker>()
            .subscriber("inventory.check")
            .assert_called_once();
        tb.broker::<LapinBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn an_order_out_of_stock_is_rejected() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<LapinBroker>()
            .message(&order(2, 50))
            .to("orders")
            .publish()
            .await?;

        tb.broker::<LapinBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn an_unanswered_order_asks_again_later() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(orders_alone()).await?;

        tb.broker::<LapinBroker>()
            .message(&order(3, 2))
            .to("orders")
            .publish()
            .await?;

        tb.broker::<LapinBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::retry_after(RETRY));
        tb.shutdown().await?;
        Ok(())
    }
}
