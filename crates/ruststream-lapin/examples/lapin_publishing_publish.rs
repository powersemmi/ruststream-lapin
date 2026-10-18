//! The fire-and-forget publish policy: a reply goes where its type says, and a one-off publish at
//! startup goes through the same policy.
//!
//! ```text
//! cargo test --example lapin_publishing_publish --features testing
//! cargo run --example lapin_publishing_publish -- run
//! ```

use ruststream_lapin::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "confirmations")]
struct Confirmation {
    order_id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "services.started")]
struct Started {
    service: String,
}

// --8<-- [start:publish]
/// Every order is answered with a confirmation, routed under the name its type declares.
#[subscriber(RabbitQueue::new("orders"), reply)]
async fn confirm(order: &Order) -> Confirmation {
    tracing::info!(order.id, "order confirmed");
    Confirmation { order_id: order.id }
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        LapinBroker::new("amqp://localhost:5672"),
        |b| {
            b.include(confirm).out_reply(Publish::default());
            // Once the subscriptions are open, the service announces itself.
            b.after_startup(Publish::default(), async move |publisher| {
                let started = Started {
                    service: "orders".into(),
                };
                publisher.message(&started).publish().await
            });
        },
    )
}
// --8<-- [end:publish]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_lapin::prelude::*;

    use super::{Confirmation, Order, Started, app};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_service_announces_itself_at_startup() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<LapinBroker>()
            .published::<Started>("services.started")
            .assert_called_once()
            .with(&Started {
                service: "orders".into(),
            });
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_reply_is_routed_under_its_type_name() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<LapinBroker>()
            .message(&Order { id: 7 })
            .publish()
            .await?;

        tb.broker::<LapinBroker>()
            .published::<Confirmation>("confirmations")
            .assert_called_once()
            .with(&Confirmation { order_id: 7 })
            .assert_options_default();
        tb.shutdown().await?;
        Ok(())
    }
}
