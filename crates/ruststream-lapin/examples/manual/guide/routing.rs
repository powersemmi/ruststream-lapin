//! The shop of the Exchanges and routing guide, written without the macros: each handler is a
//! type with an `impl Handle`, mounted with the `subscriber` constructor, and `main` is written by
//! hand.
//!
//! ```text
//! docker compose -f docker-compose.test.yml up -d rabbitmq
//! cargo run --example manual_guide_routing --features testing,ruststream/logging -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_lapin::context::AmqpContext;
use ruststream_lapin::prelude::*;
use serde::{Deserialize, Serialize};

// --8<-- [start:topic]
#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
    region: String,
}

/// An order event; its routing key is `order.placed.<region>`.
#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct OrderPlaced {
    id: u64,
    region: String,
}

/// Takes an order from the `orders` queue and announces it on the `events` exchange.
struct PlaceOrder;

impl<Events> Handle<Order, (), Outs<(Events,)>> for PlaceOrder
where
    Events: OutEntry<DefaultSlot, Wire: Publisher>,
{
    async fn handle(
        &self,
        order: &Order,
        outs: &Outs<(Events,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let placed = OrderPlaced {
            id: order.id,
            region: order.region.clone(),
        };
        let key = format!("order.placed.{}", order.region);
        let sent = outs
            .get(DefaultSlot)
            .message(&placed)
            .to(&key)
            .publish()
            .await;
        sent.map_err(|_| HandlerOutcome::retry())
    }
}

/// Every order event of every region.
struct NotifyCustomer;

impl Handle<OrderPlaced, (), (), AmqpContext> for NotifyCustomer {
    fn handle(
        &self,
        event: &OrderPlaced,
        _outs: &(),
        ctx: &mut Context<'_, AmqpContext>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(event.id, routing_key = %ctx.context(RoutingKey), "customer notified");
        ready(Ok(()))
    }
}

/// The events of the EU region only.
struct ReserveStock;

impl Handle<OrderPlaced> for ReserveStock {
    fn handle(
        &self,
        event: &OrderPlaced,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(event.id, "stock reserved in the EU warehouse");
        ready(Ok(()))
    }
}

/// Every event on the exchange, whatever its key.
struct Audit;

impl Handle<OrderPlaced, (), (), AmqpContext> for Audit {
    fn handle(
        &self,
        event: &OrderPlaced,
        _outs: &(),
        ctx: &mut Context<'_, AmqpContext>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(event.id, routing_key = %ctx.context(RoutingKey), "event audited");
        ready(Ok(()))
    }
}
// --8<-- [end:topic]

// --8<-- [start:fanout]
#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct PriceChange {
    sku: String,
    price_cents: u64,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "price.changed")]
struct PriceChanged {
    sku: String,
    price_cents: u64,
}

/// Applies a price change from the catalog and announces it to every reader of `prices`.
struct ChangePrice;

impl Handle<PriceChange, PriceChanged> for ChangePrice {
    fn handle(
        &self,
        change: &PriceChange,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<PriceChanged, HandlerOutcome>> {
        tracing::info!(change.sku, change.price_cents, "price changed");
        ready(Ok(PriceChanged {
            sku: change.sku.clone(),
            price_cents: change.price_cents,
        }))
    }
}

struct RefreshStorefront;

impl Handle<PriceChanged> for RefreshStorefront {
    fn handle(
        &self,
        change: &PriceChanged,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(change.sku, "storefront price refreshed");
        ready(Ok(()))
    }
}

struct Reindex;

impl Handle<PriceChanged> for Reindex {
    fn handle(
        &self,
        change: &PriceChanged,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(change.sku, "search index updated");
        ready(Ok(()))
    }
}
// --8<-- [end:fanout]

// --8<-- [start:headers]
#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct PaidOrder {
    id: u64,
    region: String,
    tier: String,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "shipment")]
struct Shipment {
    order_id: u64,
    region: String,
    tier: String,
}

/// Ships a paid order, with its region and tier as headers, which the exchange routes on.
struct ShipOrder;

impl<Shipments> Handle<PaidOrder, (), Outs<(Shipments,)>> for ShipOrder
where
    Shipments: OutEntry<DefaultSlot, Wire: Publisher>,
{
    async fn handle(
        &self,
        order: &PaidOrder,
        outs: &Outs<(Shipments,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let shipment = Shipment {
            order_id: order.id,
            region: order.region.clone(),
            tier: order.tier.clone(),
        };
        let mut attributes = HeaderMap::new();
        attributes.insert("region", order.region.clone());
        attributes.insert("tier", order.tier.clone());
        let sent = outs
            .get(DefaultSlot)
            .message(&shipment)
            .with_headers(attributes)
            .publish()
            .await;
        sent.map_err(|_| HandlerOutcome::retry())
    }
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
struct BookExpress;

impl Handle<Shipment> for BookExpress {
    fn handle(
        &self,
        shipment: &Shipment,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(shipment.order_id, "express courier booked");
        ready(Ok(()))
    }
}

/// Shipments that are gold or in the EU: one header is enough.
struct Insure;

impl Handle<Shipment> for Insure {
    fn handle(
        &self,
        shipment: &Shipment,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(shipment.order_id, "shipment insured");
        ready(Ok(()))
    }
}
// --8<-- [end:headers]

// --8<-- [start:app]
fn app() -> impl App {
    let broker = LapinBroker::new("amqp://localhost:5672").declare_topology(true);
    let events = RabbitExchange::topic("events");
    let prices = RabbitExchange::fanout("prices");
    let shipments = RabbitExchange::headers("shipments");
    RustStream::new(AppInfo::new("shop", "0.1.0")).with_broker(broker, |b| {
        b.include(subscriber("orders", PlaceOrder).build())
            .out(DefaultSlot, Publish::default().exchange("events"))
            .build();
        b.include(
            subscriber(
                RabbitQueue::new("notifications.orders").bind(events.clone(), "order.placed.*"),
                NotifyCustomer,
            )
            .build(),
        );
        b.include(
            subscriber(
                RabbitQueue::new("warehouse.eu").bind(events.clone(), "*.*.eu"),
                ReserveStock,
            )
            .build(),
        );
        b.include(subscriber(RabbitQueue::new("audit").bind(events, "#"), Audit).build());

        b.include(subscriber("catalog.prices", ChangePrice).reply().build())
            .out_reply(Publish::default().exchange("prices"));
        b.include(
            subscriber(
                RabbitQueue::new("storefront.prices").bind(prices.clone(), ""),
                RefreshStorefront,
            )
            .build(),
        );
        b.include(subscriber(RabbitQueue::new("search.prices").bind(prices, ""), Reindex).build());

        b.include(subscriber("orders.paid", ShipOrder).build())
            .out(DefaultSlot, Publish::default().exchange("shipments"))
            .build();
        b.include(
            subscriber(
                RabbitQueue::new("couriers.express").bind_with(
                    shipments.clone(),
                    "",
                    gold_in_eu("all"),
                ),
                BookExpress,
            )
            .build(),
        );
        b.include(
            subscriber(
                RabbitQueue::new("insurance").bind_with(shipments, "", gold_in_eu("any")),
                Insure,
            )
            .build(),
        );
    })
}

fn main() -> ExitCode {
    run_main(app)
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
