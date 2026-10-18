//! The responder side of RPC over direct reply-to: an ordinary reply handler whose reply goes to
//! the address each request named, with the request's correlation id.
//!
//! ```text
//! cargo test --example lapin_request_direct_reply_to --features testing
//! cargo run --example lapin_request_direct_reply_to -- run
//! ```

use ruststream_lapin::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "inventory.check")]
struct CheckStock {
    sku: String,
}

// --8<-- [start:responder]
/// The reply goes wherever the request asks, so its type declares no destination.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Stock {
    sku: String,
    available: bool,
}

#[subscriber(RabbitQueue::new("inventory.check"), reply("inventory.check.unrouted"))]
async fn check(ask: &CheckStock) -> Stock {
    Stock {
        sku: ask.sku.clone(),
        available: ask.sku != "unobtainium",
    }
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("inventory", "0.1.0")).with_broker(
        LapinBroker::new("amqp://localhost:5672"),
        |b| {
            b.include(check)
                .out_reply(Publish::default())
                .transform(DirectReplyTo);
        },
    )
}
// --8<-- [end:responder]

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
