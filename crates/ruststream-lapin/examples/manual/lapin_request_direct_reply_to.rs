//! The service of `examples/lapin_request_direct_reply_to.rs` written without `#[subscriber]`:
//! the reply body implements `Handle`, and the reply position takes the same transform.
//!
//! ```text
//! cargo test --example manual_lapin_request_direct_reply_to --features testing
//! cargo run --example manual_lapin_request_direct_reply_to -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_lapin::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "inventory.check")]
struct CheckStock {
    sku: String,
}

// --8<-- [start:responder]
/// The reply goes wherever the request asks, so its type declares no destination.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Stock {
    sku: String,
    available: bool,
}

struct Check;

impl Handle<CheckStock, Stock> for Check {
    fn handle(
        &self,
        ask: &CheckStock,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Stock, HandlerOutcome>> {
        ready(Ok(Stock {
            sku: ask.sku.clone(),
            available: ask.sku != "unobtainium",
        }))
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("inventory", "0.1.0")).with_broker(
        LapinBroker::new("amqp://localhost:5672"),
        |b| {
            let check = subscriber(RabbitQueue::new("inventory.check"), Check);
            b.include(check.reply().to("inventory.check.unrouted").build())
                .out_reply(Publish::default())
                .transform(DirectReplyTo);
        },
    )
}
// --8<-- [end:responder]

fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_lapin::prelude::*;

    use super::{CheckStock, Stock, app};

    fn widget() -> CheckStock {
        CheckStock {
            sku: "widget".into(),
        }
    }

    fn in_stock() -> Stock {
        Stock {
            sku: "widget".into(),
            available: true,
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_reply_goes_where_the_request_asked() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let mut headers = HeaderMap::new();
        headers.insert("reply-to", "client.replies");
        headers.insert("correlation-id", "c-1");
        tb.broker::<LapinBroker>()
            .message(&widget())
            .with_headers(headers)
            .publish()
            .await?;

        tb.broker::<LapinBroker>()
            .published::<Stock>("client.replies")
            .assert_called_once()
            .with(&in_stock())
            .with_header("correlation-id", "c-1");
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_request_without_an_address_is_answered_at_the_mount_name()
    -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<LapinBroker>()
            .message(&widget())
            .publish()
            .await?;

        tb.broker::<LapinBroker>()
            .published::<Stock>("inventory.check.unrouted")
            .assert_called_once()
            .with(&in_stock());
        tb.shutdown().await?;
        Ok(())
    }
}
