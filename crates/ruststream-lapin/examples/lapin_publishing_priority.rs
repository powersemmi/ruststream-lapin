//! The AMQP priority of one message or of every message of a policy, and the delivery that
//! reports it to the consumer.
//!
//! ```text
//! cargo test --example lapin_publishing_priority --features testing
//! cargo run --example lapin_publishing_priority -- run
//! ```

use ruststream::OutSlot;
use ruststream_lapin::PRIORITY_HEADER;
use ruststream_lapin::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "readings")]
struct Reading {
    sensor: String,
    celsius: i32,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "alerts")]
struct Alert {
    sensor: String,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "pager")]
struct Page {
    sensor: String,
}

#[derive(OutSlot)]
#[publishes(Alert)]
struct Alerts;

// --8<-- [start:priority]
/// A reading over 90 degrees raises an alert at priority 9; a warm one takes the policy's 1.
#[subscriber(RabbitQueue::new("readings"))]
async fn watch(
    reading: &Reading,
    Out(alerts): Out<impl Publisher<Options = LapinPublishOptions>, Alerts>,
) -> HandlerOutcome {
    if reading.celsius < 60 {
        return HandlerOutcome::ack();
    }
    let alert = Alert {
        sensor: reading.sensor.clone(),
    };
    let sent = if reading.celsius > 90 {
        alerts.message(&alert).priority(9).publish().await
    } else {
        alerts.message(&alert).publish().await
    };
    if sent.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

/// The alerts queue orders by priority, and the consumer pages someone for the urgent ones.
#[subscriber(RabbitQueue::new("alerts").argument("x-max-priority", AMQPValue::LongLongInt(10)))]
async fn triage(
    alert: &Alert,
    ctx: &mut Context<'_>,
    Out(pager): Out<impl Publisher>,
) -> HandlerOutcome {
    let priority = ctx.headers().get_str(PRIORITY_HEADER).unwrap_or("0");
    if priority != "9" {
        tracing::info!(alert.sensor, priority, "alert logged");
        return HandlerOutcome::ack();
    }
    let page = Page {
        sensor: alert.sensor.clone(),
    };
    if pager.message(&page).publish().await.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    let broker = LapinBroker::new("amqp://localhost:5672").declare_topology(true);
    RustStream::new(AppInfo::new("monitoring", "0.1.0")).with_broker(broker, |b| {
        b.include(watch)
            .out(Alerts, Publish::default().priority(1))
            .build();
        b.include(triage)
            .out(DefaultSlot, Publish::default())
            .build();
    })
}
// --8<-- [end:priority]

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
