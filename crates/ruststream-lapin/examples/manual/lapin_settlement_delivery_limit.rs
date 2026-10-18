//! The service of `examples/lapin_settlement_delivery_limit.rs` written without `#[subscriber]`:
//! each body implements `Handle`, and the quorum descriptor is the same value.
//!
//! ```text
//! cargo test --example manual_lapin_settlement_delivery_limit --features testing
//! cargo run --example manual_lapin_settlement_delivery_limit -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_lapin::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "charges")]
struct Charge {
    id: u64,
}

// --8<-- [start:limit]
/// The provider refuses this charge every time. The queue returns it four times, so it is
/// delivered five times, and then the queue moves it to `charges.dlx`.
struct TakePayment;

impl Handle<Charge> for TakePayment {
    fn handle(
        &self,
        charge: &Charge,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::warn!(charge.id, "provider refused the charge");
        ready(Err(HandlerOutcome::retry()))
    }
}

/// The charges the queue gave up on.
struct Park;

impl Handle<Charge> for Park {
    fn handle(
        &self,
        charge: &Charge,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::error!(charge.id, "charge parked");
        ready(Ok(()))
    }
}

fn app() -> impl App {
    let broker = LapinBroker::new("amqp://localhost:5672").declare_topology(true);
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(broker, |b| {
        let charges = RabbitQuorumQueue::new("charges")
            .delivery_limit(4)
            .dead_letter_exchange("charges.dlx");
        let parked =
            RabbitQueue::new("charges.parked").bind(RabbitExchange::fanout("charges.dlx"), "");
        b.include(subscriber(charges, TakePayment).build());
        b.include(subscriber(parked, Park).build());
    })
}
// --8<-- [end:limit]

fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_lapin::prelude::*;

    use super::{Charge, app};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_fifth_delivery_is_the_last() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<LapinBroker>()
            .message(&Charge { id: 5 })
            .publish()
            .await?;

        tb.broker::<LapinBroker>()
            .subscriber("charges")
            .assert_called(5)
            .settled(HandlerOutcome::retry());
        tb.broker::<LapinBroker>()
            .subscriber("charges.parked")
            .assert_called_once()
            .with(&Charge { id: 5 });
        // The queue moved the charge; the service published nothing.
        tb.broker::<LapinBroker>()
            .published::<Charge>("charges")
            .assert_called_once();
        tb.shutdown().await?;
        Ok(())
    }
}
