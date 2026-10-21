//! The service of `examples/lapin_subscriptions_lanes.rs` without the macros: the handler is a named type and the lanes a mount step, and `main` is
//! written by hand.
//!
//! ```text
//! cargo test --example manual_lapin_subscriptions_lanes --features testing
//! ```

use std::future::ready;
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;

use serde::{Deserialize, Serialize};

// --8<-- [start:lanes]
use ruststream_lapin::prelude::*;

struct Place;

impl Handle<Order> for Place {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, order.tenant, "order placed");
        ready(Ok(()))
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        LapinBroker::new("amqp://localhost:5672"),
        |b| {
            // Eight lanes: one tenant's orders never run at the same time, and different
            // tenants run side by side.
            let orders = RabbitQueue::new("orders");
            b.include(
                subscriber(orders, Place)
                    .workers_by_key(nonzero!(8))
                    .build(),
            );
        },
    )
}
// --8<-- [end:lanes]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
    tenant: String,
}

// `#[outgoing(name = "orders")]` by hand: the type names the queue it is published to.
impl OutgoingDestination for Order {
    type Form = FixedName;
    const DESTINATION: &'static str = "orders";
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::Duration;
    use tokio::time::sleep;

    use ruststream::testing::TestApp;
    use ruststream_lapin::PARTITION_KEY_HEADER;
    use ruststream_lapin::prelude::*;
    use serde::{Deserialize, Serialize};

    use super::{Order, app};

    /// An order on the queue whose lanes do real work.
    #[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
    #[outgoing(name = "orders.timed")]
    struct Timed {
        id: u64,
        tenant: String,
    }

    /// What a lane finished, in the order lanes finish.
    #[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
    #[outgoing(name = "orders.placed")]
    struct Placed {
        id: u64,
        tenant: String,
    }

    #[derive(OutSlot)]
    #[publishes(Placed)]
    struct Placements;

    /// The lanes of the page with real work in them: the first order takes longer than the rest.
    #[subscriber(RabbitQueue::new("orders.timed"), workers(8, by_key))]
    async fn place_timed(
        order: &Timed,
        Out(placements): Out<impl Publisher, Placements>,
    ) -> HandlerOutcome {
        let work = if order.id == 1 { 30 } else { 1 };
        sleep(Duration::from_millis(work)).await;
        let placed = Placed {
            id: order.id,
            tenant: order.tenant.clone(),
        };
        if placements.message(&placed).publish().await.is_err() {
            return HandlerOutcome::retry();
        }
        HandlerOutcome::ack()
    }

    fn timed() -> impl App<State = ()> {
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
            LapinBroker::new("amqp://localhost:5672"),
            |b| {
                b.include(place_timed)
                    .out(Placements, Publish::default())
                    .build();
            },
        )
    }

    #[tokio::test]
    async fn the_lanes_take_every_order() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        for (id, tenant) in [(1, "acme"), (2, "globex")] {
            let mut key = HeaderMap::new();
            key.insert(PARTITION_KEY_HEADER, tenant);
            tb.broker::<LapinBroker>()
                .message(&Order {
                    id,
                    tenant: tenant.to_owned(),
                })
                .with_headers(key)
                .publish()
                .await?;
        }

        tb.broker::<LapinBroker>()
            .subscriber("orders")
            .assert_called(2)
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    // All four orders are in the queue before the first one finishes. The slow first `acme` order
    // holds back the other two `acme` orders on its lane, and `globex` finishes beside it.
    #[tokio::test(start_paused = true)]
    async fn one_tenants_orders_finish_in_publish_order() -> Result<(), Box<dyn Error>> {
        let mut publisher = None;
        let tb = TestApp::with_state(timed(), |brokers| {
            publisher = Some(
                brokers
                    .broker::<LapinBroker>()
                    .publisher(Publish::default()),
            );
        })
        .await?;
        let publisher = publisher.ok_or("the state builder ran")?;
        for (id, tenant) in [(1, "acme"), (2, "globex"), (3, "acme"), (4, "acme")] {
            let mut key = HeaderMap::new();
            key.insert(PARTITION_KEY_HEADER, tenant);
            publisher
                .message(&Timed {
                    id,
                    tenant: tenant.to_owned(),
                })
                .with_headers(key)
                .publish()
                .await?;
        }
        tb.advance(Duration::from_millis(100)).await?;

        let finished: Vec<u64> = tb
            .broker::<LapinBroker>()
            .published::<Placed>("orders.placed")
            .assert_called(4)
            .decoded()
            .into_iter()
            .map(|placed| placed.id)
            .collect();
        assert_eq!(finished, [2, 1, 3, 4]);
        tb.shutdown().await?;
        Ok(())
    }
}
