//! The service of `examples/lapin_publishing_priority.rs` written without `#[subscriber]`: the
//! bodies implement `Handle`, and the slots take the same policies.
//!
//! ```text
//! cargo test --example manual_lapin_publishing_priority --features testing
//! cargo run --example manual_lapin_publishing_priority -- run
//! ```

use std::process::ExitCode;

use ruststream::OutSlot;
use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_lapin::PRIORITY_HEADER;
use ruststream_lapin::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "readings")]
struct Reading {
    sensor: String,
    celsius: i32,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "alerts")]
struct Alert {
    sensor: String,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "pager")]
struct Page {
    sensor: String,
}

#[derive(OutSlot)]
#[publishes(Alert)]
struct Alerts;

// --8<-- [start:priority]
/// A reading over 90 degrees raises an alert at priority 9; a warm one takes the policy's 1.
struct Watch;

impl<Egress> Handle<Reading, (), Outs<(Egress,)>> for Watch
where
    Egress: OutEntry<Alerts, Wire: Publisher<Options = LapinPublishOptions>>,
{
    async fn handle(
        &self,
        reading: &Reading,
        outs: &Outs<(Egress,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        if reading.celsius < 60 {
            return Ok(());
        }
        let alert = Alert {
            sensor: reading.sensor.clone(),
        };
        let alerts = outs.get(Alerts);
        let sent = if reading.celsius > 90 {
            alerts.message(&alert).priority(9).publish().await
        } else {
            alerts.message(&alert).publish().await
        };
        if sent.is_err() {
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
}

/// The alerts queue orders by priority, and the consumer pages someone for the urgent ones.
struct Triage;

impl<Egress> Handle<Alert, (), Outs<(Egress,)>> for Triage
where
    Egress: OutEntry<DefaultSlot, Wire: Publisher>,
{
    async fn handle(
        &self,
        alert: &Alert,
        outs: &Outs<(Egress,)>,
        ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let priority = ctx.headers().get_str(PRIORITY_HEADER).unwrap_or("0");
        if priority != "9" {
            tracing::info!(alert.sensor, priority, "alert logged");
            return Ok(());
        }
        let page = Page {
            sensor: alert.sensor.clone(),
        };
        let pager = outs.get(DefaultSlot);
        if pager.message(&page).publish().await.is_err() {
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
}

fn app() -> impl App {
    let broker = LapinBroker::new("amqp://localhost:5672").declare_topology(true);
    RustStream::new(AppInfo::new("monitoring", "0.1.0")).with_broker(broker, |b| {
        let alerts =
            RabbitQueue::new("alerts").argument("x-max-priority", AMQPValue::LongLongInt(10));
        b.include(subscriber(RabbitQueue::new("readings"), Watch).build())
            .out(Alerts, Publish::default().priority(1))
            .build();
        b.include(subscriber(alerts, Triage).build())
            .out(DefaultSlot, Publish::default())
            .build();
    })
}
// --8<-- [end:priority]

fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_lapin::prelude::*;

    use super::{Alerts, Page, Reading, app};

    fn reading(celsius: i32) -> Reading {
        Reading {
            sensor: "boiler".into(),
            celsius,
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_urgent_alert_reaches_the_consumer_at_priority_nine() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<LapinBroker>()
            .message(&reading(95))
            .publish()
            .await?;

        tb.out::<Alerts>()
            .assert_called_once()
            .with_options(&LapinPublishOptions {
                priority: Some(9),
                ..LapinPublishOptions::default()
            });
        tb.broker::<LapinBroker>()
            .published::<Page>("pager")
            .assert_called_once()
            .with(&Page {
                sensor: "boiler".into(),
            });
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_warm_alert_carries_the_policy_priority() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<LapinBroker>()
            .message(&reading(70))
            .publish()
            .await?;

        tb.out::<Alerts>()
            .assert_called_once()
            .assert_options_default();
        tb.broker::<LapinBroker>()
            .subscriber("alerts")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.broker::<LapinBroker>()
            .published::<Page>("pager")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }
}
