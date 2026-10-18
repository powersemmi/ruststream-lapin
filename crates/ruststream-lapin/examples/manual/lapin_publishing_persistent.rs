//! The service of `examples/lapin_publishing_persistent.rs` written without `#[subscriber]`: the
//! body implements `Handle` over a slot whose publisher takes this crate's options.
//!
//! ```text
//! cargo test --example manual_lapin_publishing_persistent --features testing
//! cargo run --example manual_lapin_publishing_persistent -- run
//! ```

use std::process::ExitCode;

use ruststream::OutSlot;
use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_lapin::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "meters")]
struct MeterTick {
    meter: String,
    kwh: u64,
    billing: bool,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
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
struct Record;

impl<Egress> Handle<MeterTick, (), Outs<(Egress,)>> for Record
where
    Egress: OutEntry<Readings, Wire: Publisher<Options = LapinPublishOptions>>,
{
    async fn handle(
        &self,
        tick: &MeterTick,
        outs: &Outs<(Egress,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let reading = Reading {
            meter: tick.meter.clone(),
            kwh: tick.kwh,
        };
        let readings = outs.get(Readings);
        let sent = if tick.billing {
            readings.message(&reading).persistent(true).publish().await
        } else {
            readings.message(&reading).publish().await
        };
        if sent.is_err() {
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("metering", "0.1.0")).with_broker(
        LapinBroker::new("amqp://localhost:5672"),
        |b| {
            b.include(subscriber(RabbitQueue::new("meters"), Record).build())
                .out(Readings, Publish::default().persistent(false))
                .build();
        },
    )
}
// --8<-- [end:persistent]

fn main() -> ExitCode {
    run_main(app)
}

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
