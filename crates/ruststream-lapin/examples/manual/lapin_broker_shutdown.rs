//! The service of `examples/lapin_broker_shutdown.rs` without the macros: the handler is a named
//! type and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_lapin_broker_shutdown --features testing
//! ```

use std::convert::Infallible;
use std::future::ready;
use std::process::ExitCode;
use std::time::Duration;
use tokio::time::interval;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use ruststream_lapin::LapinPublisher;
use ruststream_lapin::prelude::*;

struct Place;

impl Handle<Order> for Place {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, "order placed");
        ready(Ok(()))
    }
}

// --8<-- [start:app]
/// Announces the service every 30 seconds until a publish fails; after shutdown that failure is
/// `AmqpError::Closed`.
async fn heartbeat(publisher: LapinPublisher) {
    let mut ticks = interval(Duration::from_secs(30));
    loop {
        ticks.tick().await;
        let beat = Heartbeat {
            service: "orders".to_owned(),
        };
        if let Err(err) = publisher.message(&beat).publish().await {
            tracing::info!(error = %err, "heartbeat stopped");
            return;
        }
    }
}

fn app() -> impl App<State = ()> {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        LapinBroker::new("amqp://localhost:5672"),
        |b| {
            b.include(subscriber("orders", Place).build());
            // The task keeps its publisher past the service's shutdown.
            b.after_startup(Publish::default(), async move |publisher| {
                tokio::spawn(heartbeat(publisher));
                Ok::<_, Infallible>(())
            });
        },
    )
}
// --8<-- [end:app]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
}

// `#[outgoing(name = "orders")]` by hand: an order names the queue it is published to.
impl OutgoingDestination for Order {
    type Form = FixedName;
    const DESTINATION: &'static str = "orders";
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Heartbeat {
    service: String,
}

// `#[outgoing(name = "heartbeats")]` by hand.
impl OutgoingDestination for Heartbeat {
    type Form = FixedName;
    const DESTINATION: &'static str = "heartbeats";
}

impl MessageHeaders for Heartbeat {
    type Contract = NoHeaders;
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::runtime::PublishError;
    use ruststream::testing::TestApp;
    use ruststream_lapin::AmqpError;
    use ruststream_lapin::prelude::*;

    use super::{Heartbeat, app};

    #[tokio::test]
    async fn a_publisher_kept_past_shutdown_reports_closed() -> Result<(), Box<dyn Error>> {
        let mut kept = None;
        let tb = TestApp::with_state(app(), |brokers| {
            kept = Some(
                brokers
                    .broker::<LapinBroker>()
                    .publisher(Publish::default()),
            );
        })
        .await?;
        tb.shutdown().await?;

        let publisher = kept.ok_or("the state builder ran")?;
        let late = publisher
            .message(&Heartbeat {
                service: "orders".to_owned(),
            })
            .publish()
            .await;
        assert!(
            matches!(late, Err(PublishError::Publish(AmqpError::Closed { .. }))),
            "{late:?}"
        );
        Ok(())
    }
}
