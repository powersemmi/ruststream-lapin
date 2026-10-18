//! The service of `examples/lapin_publishing_publish.rs` written without `#[subscriber]`: the
//! reply body implements `Handle`, and the mount site pairs the same publish policy.
//!
//! ```text
//! cargo test --example manual_lapin_publishing_publish --features testing
//! cargo run --example manual_lapin_publishing_publish -- run
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
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "confirmations")]
struct Confirmation {
    order_id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "services.started")]
struct Started {
    service: String,
}

// --8<-- [start:publish]
/// Every order is answered with a confirmation, routed under the name its type declares.
struct Confirm;

impl Handle<Order, Confirmation> for Confirm {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Confirmation, HandlerOutcome>> {
        tracing::info!(order.id, "order confirmed");
        ready(Ok(Confirmation { order_id: order.id }))
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        LapinBroker::new("amqp://localhost:5672"),
        |b| {
            let confirm = subscriber(RabbitQueue::new("orders"), Confirm);
            b.include(confirm.reply().build())
                .out_reply(Publish::default());
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

fn main() -> ExitCode {
    run_main(app)
}

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
