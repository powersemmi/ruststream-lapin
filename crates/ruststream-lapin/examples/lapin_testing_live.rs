//! One test body, two modes: in process under `TestApp::start`, and against a running `RabbitMQ`
//! under `TestApp::start_live`. The live test skips without `AMQP_TEST_URL`, and
//! `RUSTSTREAM_REQUIRE_LIVE=1` turns that skip into a failure.
//!
//! ```text
//! cargo test --example lapin_testing_live --features testing
//! AMQP_TEST_URL=amqp://127.0.0.1:5672 RUSTSTREAM_REQUIRE_LIVE=1 \
//!     cargo test --example lapin_testing_live --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use serde::{Deserialize, Serialize};

use ruststream_lapin::prelude::*;

#[cfg(test)]
#[path = "../tests/live/mod.rs"]
mod live;

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "testing-live.payments")]
struct Payment {
    amount: u64,
}

/// The queue lives as long as the connection that declared it, so no run reads what another left.
#[subscriber(RabbitQueue::new("testing-live.payments").durable(false).exclusive(true))]
async fn accept(payment: &Payment) -> HandlerOutcome {
    if payment.amount == 0 {
        return HandlerOutcome::drop();
    }
    HandlerOutcome::ack()
}

/// The service on the server at `uri`, declaring the queue it consumes.
fn app(uri: &str) -> RustStream {
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(
        LapinBroker::new(uri).declare_topology(true),
        |b| {
            b.include(accept);
        },
    )
}

#[ruststream::app]
fn service() -> RustStream {
    app("amqp://localhost:5672")
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_lapin::prelude::*;

    use super::{Payment, app, live};

    // --8<-- [start:start-live]
    /// The body both modes run; only the start call differs.
    async fn accepts_a_payment(tb: TestApp<()>) -> Result<(), Box<dyn Error>> {
        tb.broker::<LapinBroker>()
            .message(&Payment { amount: 100 })
            .publish()
            .await?;

        tb.broker::<LapinBroker>()
            .subscriber("testing-live.payments")
            .assert_called_once()
            .with(&Payment { amount: 100 })
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn in_process() -> Result<(), Box<dyn Error>> {
        accepts_a_payment(TestApp::start(app("amqp://rabbit:5672")).await?).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn against_the_stand() -> Result<(), Box<dyn Error>> {
        let Some(uri) = live::url("AMQP_TEST_URL") else {
            return Ok(());
        };
        accepts_a_payment(TestApp::start_live(app(&uri)).await?).await
    }
    // --8<-- [end:start-live]
}
