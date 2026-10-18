//! A publisher that outlives the connection: a heartbeat task the service starts at startup, which
//! ends once its publisher reports the connection closed.
//!
//! ```text
//! cargo test --example lapin_broker_shutdown --features testing
//! ```

use std::convert::Infallible;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use ruststream_lapin::LapinPublisher;
use ruststream_lapin::prelude::*;

#[subscriber("orders")]
async fn place(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "order placed");
    HandlerOutcome::ack()
}

// --8<-- [start:app]
/// Announces the service every 30 seconds until a publish fails; after shutdown that failure is
/// `AmqpError::Closed`.
async fn heartbeat(publisher: LapinPublisher) {
    let mut ticks = tokio::time::interval(Duration::from_secs(30));
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

#[ruststream::app]
fn app() -> impl App<State = ()> {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        LapinBroker::new("amqp://localhost:5672"),
        |b| {
            b.include(place);
            // The task keeps its publisher past the service's shutdown.
            b.after_startup(Publish::default(), async move |publisher| {
                tokio::spawn(heartbeat(publisher));
                Ok::<_, Infallible>(())
            });
        },
    )
}
// --8<-- [end:app]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "heartbeats")]
struct Heartbeat {
    service: String,
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
