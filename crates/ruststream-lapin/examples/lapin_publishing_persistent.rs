//! The AMQP delivery mode: a policy whose readings are transient, and the one reading a handler
//! marks persistent.
//!
//! ```text
//! cargo test --example lapin_publishing_persistent --features testing
//! cargo run --example lapin_publishing_persistent -- run
//! ```

use ruststream::OutSlot;
use ruststream_lapin::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "meters")]
struct MeterTick {
    meter: String,
    kwh: u64,
    billing: bool,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "readings")]
struct Reading {
    meter: String,
    kwh: u64,
}

// --8<-- [start:persistent]
#[derive(OutSlot)]
#[publishes(Reading)]
struct Readings;

/// Live readings are transient: a broker restart may lose them, and the next tick replaces them.
/// The reading a bill is made from is persistent.
#[subscriber(RabbitQueue::new("meters"))]
async fn record(
    tick: &MeterTick,
    Out(readings): Out<impl Publisher<Options = LapinPublishOptions>, Readings>,
) -> HandlerOutcome {
    let reading = Reading {
        meter: tick.meter.clone(),
        kwh: tick.kwh,
    };
    let sent = if tick.billing {
        readings.message(&reading).persistent(true).publish().await
    } else {
        readings.message(&reading).publish().await
    };
    if sent.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("metering", "0.1.0")).with_broker(
        LapinBroker::new("amqp://localhost:5672"),
        |b| {
            b.include(record)
                .out(Readings, Publish::default().persistent(false))
                .build();
        },
    )
}
// --8<-- [end:persistent]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_lapin::prelude::*;

    use super::{MeterTick, Readings, app};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_billing_reading_is_persistent() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<LapinBroker>()
            .message(&MeterTick {
                meter: "m-1".into(),
                kwh: 12,
                billing: true,
            })
            .publish()
            .await?;

        tb.out::<Readings>()
            .assert_called_once()
            .with_options(&LapinPublishOptions {
                persistent: Some(true),
                ..LapinPublishOptions::default()
            });
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_live_reading_takes_the_transient_policy() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<LapinBroker>()
            .message(&MeterTick {
                meter: "m-1".into(),
                kwh: 12,
                billing: false,
            })
            .publish()
            .await?;

        tb.out::<Readings>()
            .assert_called_once()
            .assert_options_default();
        tb.shutdown().await?;
        Ok(())
    }
}
