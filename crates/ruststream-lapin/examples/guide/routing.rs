//! The shop of the Exchanges and routing guide: order events on a topic exchange, price changes on
//! a fanout exchange and shipments on a headers exchange, each read by the queues bound to it.
//!
//! ```text
//! docker compose -f docker-compose.test.yml up -d rabbitmq
//! cargo run --example guide_routing --features testing,ruststream/logging -- run
//! ```

use ruststream_lapin::prelude::*;
use serde::{Deserialize, Serialize};

// --8<-- [start:topic]
#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
struct Order {
    id: u64,
    region: String,
}

/// An order event; its routing key is `order.placed.<region>`.
#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
struct OrderPlaced {
    id: u64,
    region: String,
}

/// Takes an order from the `orders` queue and announces it on the `events` exchange.
#[subscriber("orders")]
async fn place_order(order: &Order, Out(events): Out<impl Publisher>) -> HandlerOutcome {
    let placed = OrderPlaced {
        id: order.id,
        region: order.region.clone(),
    };
    let key = format!("order.placed.{}", order.region);
    if events.message(&placed).to(&key).publish().await.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

/// Every order event of every region.
#[subscriber(RabbitQueue::new("notifications.orders")
    .bind(RabbitExchange::topic("events"), "order.placed.*"))]
async fn notify_customer(event: &OrderPlaced, Ctx(key): Ctx<RoutingKey>) -> HandlerOutcome {
    tracing::info!(event.id, routing_key = %key, "customer notified");
    HandlerOutcome::ack()
}

/// The events of the EU region only.
#[subscriber(RabbitQueue::new("warehouse.eu")
    .bind(RabbitExchange::topic("events"), "*.*.eu"))]
async fn reserve_stock(event: &OrderPlaced) -> HandlerOutcome {
    tracing::info!(event.id, "stock reserved in the EU warehouse");
    HandlerOutcome::ack()
}

/// Every event on the exchange, whatever its key.
#[subscriber(RabbitQueue::new("audit").bind(RabbitExchange::topic("events"), "#"))]
async fn audit(event: &OrderPlaced, Ctx(key): Ctx<RoutingKey>) -> HandlerOutcome {
    tracing::info!(event.id, routing_key = %key, "event audited");
    HandlerOutcome::ack()
}
// --8<-- [end:topic]

// --8<-- [start:fanout]
#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
struct PriceChange {
    sku: String,
    price_cents: u64,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
#[outgoing(name = "price.changed")]
struct PriceChanged {
    sku: String,
    price_cents: u64,
}

/// Applies a price change from the catalog and announces it to every reader of `prices`.
#[subscriber("catalog.prices", reply)]
async fn change_price(change: &PriceChange) -> PriceChanged {
    tracing::info!(change.sku, change.price_cents, "price changed");
    PriceChanged {
        sku: change.sku.clone(),
        price_cents: change.price_cents,
    }
}

#[subscriber(RabbitQueue::new("storefront.prices").bind(RabbitExchange::fanout("prices"), ""))]
async fn refresh_storefront(change: &PriceChanged) -> HandlerOutcome {
    tracing::info!(change.sku, "storefront price refreshed");
    HandlerOutcome::ack()
}

#[subscriber(RabbitQueue::new("search.prices").bind(RabbitExchange::fanout("prices"), ""))]
async fn reindex(change: &PriceChanged) -> HandlerOutcome {
    tracing::info!(change.sku, "search index updated");
    HandlerOutcome::ack()
}
// --8<-- [end:fanout]

// --8<-- [start:headers]
#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
struct PaidOrder {
    id: u64,
    region: String,
    tier: String,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
#[outgoing(name = "shipment")]
struct Shipment {
    order_id: u64,
    region: String,
    tier: String,
}

/// Ships a paid order, with its region and tier as headers, which the exchange routes on.
#[subscriber("orders.paid")]
async fn ship_order(order: &PaidOrder, Out(shipments): Out<impl Publisher>) -> HandlerOutcome {
    let shipment = Shipment {
        order_id: order.id,
        region: order.region.clone(),
        tier: order.tier.clone(),
    };
    let mut attributes = HeaderMap::new();
    attributes.insert("region", order.region.clone());
    attributes.insert("tier", order.tier.clone());
    let sent = shipments
        .message(&shipment)
        .with_headers(attributes)
        .publish()
        .await;
    if sent.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

/// The binding arguments: how to match, then the headers matched on.
fn gold_in_eu(x_match: &str) -> FieldTable {
    let mut arguments = FieldTable::default();
    arguments.insert("x-match".into(), AMQPValue::LongString(x_match.into()));
    arguments.insert("region".into(), AMQPValue::LongString("eu".into()));
    arguments.insert("tier".into(), AMQPValue::LongString("gold".into()));
    arguments
}

/// Gold shipments in the EU: both headers have to match.
#[subscriber(RabbitQueue::new("couriers.express")
    .bind_with(RabbitExchange::headers("shipments"), "", gold_in_eu("all")))]
async fn book_express(shipment: &Shipment) -> HandlerOutcome {
    tracing::info!(shipment.order_id, "express courier booked");
    HandlerOutcome::ack()
}

/// Shipments that are gold or in the EU: one header is enough.
#[subscriber(RabbitQueue::new("insurance")
    .bind_with(RabbitExchange::headers("shipments"), "", gold_in_eu("any")))]
async fn insure(shipment: &Shipment) -> HandlerOutcome {
    tracing::info!(shipment.order_id, "shipment insured");
    HandlerOutcome::ack()
}
// --8<-- [end:headers]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App {
    let broker = LapinBroker::new("amqp://localhost:5672").declare_topology(true);
    RustStream::new(AppInfo::new("shop", "0.1.0")).with_broker(broker, |b| {
        b.include(place_order)
            .out(DefaultSlot, Publish::default().exchange("events"))
            .build();
        b.include(notify_customer);
        b.include(reserve_stock);
        b.include(audit);

        b.include(change_price)
            .out_reply(Publish::default().exchange("prices"));
        b.include(refresh_storefront);
        b.include(reindex);

        b.include(ship_order)
            .out(DefaultSlot, Publish::default().exchange("shipments"))
            .build();
        b.include(book_express);
        b.include(insure);
    })
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_lapin::prelude::*;

    use super::{Order, OrderPlaced, PaidOrder, PriceChange, app};

    fn order(id: u64, region: &str) -> Order {
        Order {
            id,
            region: region.to_owned(),
        }
    }

    fn paid(id: u64, region: &str, tier: &str) -> PaidOrder {
        PaidOrder {
            id,
            region: region.to_owned(),
            tier: tier.to_owned(),
        }
    }

    #[tokio::test]
    async fn an_eu_order_reaches_every_bound_queue() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<LapinBroker>()
            .message(&order(1, "eu"))
            .to("orders")
            .publish()
            .await?;

        tb.broker::<LapinBroker>()
            .published::<OrderPlaced>("order.placed.eu")
            .assert_called_once();
        for queue in ["notifications.orders", "warehouse.eu", "audit"] {
            tb.broker::<LapinBroker>()
                .subscriber(queue)
                .assert_called_once()
                .settled(HandlerOutcome::ack());
        }
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_us_order_skips_the_eu_warehouse() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<LapinBroker>()
            .message(&order(2, "us"))
            .to("orders")
            .publish()
            .await?;

        tb.broker::<LapinBroker>()
            .subscriber("warehouse.eu")
            .assert_not_called();
        for queue in ["notifications.orders", "audit"] {
            tb.broker::<LapinBroker>()
                .subscriber(queue)
                .assert_called_once();
        }
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_price_change_reaches_every_reader() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<LapinBroker>()
            .message(&PriceChange {
                sku: "kettle".to_owned(),
                price_cents: 3_900,
            })
            .to("catalog.prices")
            .publish()
            .await?;

        for queue in ["storefront.prices", "search.prices"] {
            tb.broker::<LapinBroker>()
                .subscriber(queue)
                .assert_called_once()
                .settled(HandlerOutcome::ack());
        }
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn the_headers_decide_where_a_shipment_goes() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        for sent in [
            paid(1, "eu", "gold"),
            paid(2, "eu", "silver"),
            paid(3, "us", "silver"),
        ] {
            tb.broker::<LapinBroker>()
                .message(&sent)
                .to("orders.paid")
                .publish()
                .await?;
        }

        // `all`: only the gold order in the EU.
        tb.broker::<LapinBroker>()
            .subscriber("couriers.express")
            .assert_called_once();
        // `any`: both EU orders; the silver order in the US matches neither header.
        tb.broker::<LapinBroker>()
            .subscriber("insurance")
            .assert_called(2);
        tb.shutdown().await?;
        Ok(())
    }
}
