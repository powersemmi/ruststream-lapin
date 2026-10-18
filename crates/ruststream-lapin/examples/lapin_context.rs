//! `AmqpContext`: the exchange, the routing key, the redelivered flag and the delivery tag of a
//! delivery, read in a handler through context keys.
//!
//! ```text
//! cargo test --example lapin_context --features testing
//! ```

use serde::{Deserialize, Serialize};

// --8<-- [start:handler]
use ruststream_lapin::prelude::*;

/// Records where each order came from: its exchange and routing key, and whether the server had
/// handed it out before.
#[subscriber(RabbitQueue::new("orders"), reply)]
async fn audit(
    order: &Order,
    Ctx(exchange): Ctx<Exchange>,
    Ctx(routing_key): Ctx<RoutingKey>,
    Ctx(redelivered): Ctx<Redelivered>,
    Ctx(delivery_tag): Ctx<DeliveryTag>,
) -> AuditRecord {
    tracing::info!(order.id, %exchange, %routing_key, redelivered, delivery_tag, "order audited");
    AuditRecord {
        order_id: order.id,
        exchange,
        routing_key,
        redelivered,
    }
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        LapinBroker::new("amqp://localhost:5672"),
        |b| {
            b.include(audit).out_reply(Publish::default());
        },
    )
}
// --8<-- [end:handler]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders.audit")]
struct AuditRecord {
    order_id: u64,
    exchange: String,
    routing_key: String,
    redelivered: bool,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_lapin::prelude::*;

    use super::{AuditRecord, Order, app};

    // The harness publishes on the default exchange, whose name is empty, under the queue's name.
    #[tokio::test]
    async fn the_record_names_the_default_exchange_and_the_queue() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<LapinBroker>()
            .message(&Order { id: 7 })
            .publish()
            .await?;

        tb.broker::<LapinBroker>()
            .published::<AuditRecord>("orders.audit")
            .assert_called_once()
            .with(&AuditRecord {
                order_id: 7,
                exchange: String::new(),
                routing_key: "orders".to_owned(),
                redelivered: false,
            });
        tb.shutdown().await?;
        Ok(())
    }
}
