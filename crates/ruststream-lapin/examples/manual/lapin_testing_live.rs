//! The service and tests of `examples/lapin_testing_live.rs` without the macros: the handler is a
//! named type mounted on its queue descriptor, and `main` is written by hand. The tests are the
//! same, in process and against the server at `AMQP_TEST_URL`.
//!
//! ```text
//! cargo test --example manual_lapin_testing_live --features testing
//! AMQP_TEST_URL=amqp://127.0.0.1:5672 RUSTSTREAM_REQUIRE_LIVE=1 \
//!     cargo test --example manual_lapin_testing_live --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use ruststream_lapin::prelude::*;

#[cfg(test)]
#[path = "../../tests/live/mod.rs"]
mod live;

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Payment {
    amount: u64,
}

// `#[outgoing(name = "testing-live.payments")]` by hand: the queue a payment goes to.
impl OutgoingDestination for Payment {
    type Form = FixedName;
    const DESTINATION: &'static str = "testing-live.payments";
}

impl MessageHeaders for Payment {
    type Contract = NoHeaders;
}

struct Accept;

impl Handle<Payment> for Accept {
    fn handle(
        &self,
        payment: &Payment,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        if payment.amount == 0 {
            return ready(Err(HandlerOutcome::drop()));
        }
        ready(Ok(()))
    }
}

/// The service on the server at `uri`, declaring the queue it consumes.
fn app(uri: &str) -> RustStream {
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(
        LapinBroker::new(uri).declare_topology(true),
        |b| {
            // The queue lives as long as the connection that declared it, so no run reads what
            // another left.
            let payments = RabbitQueue::new("testing-live.payments")
                .durable(false)
                .exclusive(true);
            b.include(subscriber(payments, Accept).build());
        },
    )
}

fn service() -> RustStream {
    app("amqp://localhost:5672")
}

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(service)
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
