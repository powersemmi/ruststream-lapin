//! The service of `examples/lapin_subscriptions_prefetch.rs` without the macros: the handlers are named types, and `main` is
//! written by hand.
//!
//! ```text
//! cargo test --example manual_lapin_subscriptions_prefetch --features testing
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;

use serde::{Deserialize, Serialize};

#[cfg(test)]
#[path = "../../tests/live/mod.rs"]
mod live;

// --8<-- [start:prefetch]
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

/// A report takes minutes to render, so this instance holds one at a time and leaves the rest
/// to the other instances.
struct Render;

impl Handle<Report> for Render {
    fn handle(
        &self,
        report: &Report,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(report.id, "report rendered");
        ready(Ok(()))
    }
}

fn app() -> impl App {
    // Every subscription holds up to 64 unacknowledged deliveries, unless its descriptor says
    // otherwise.
    let broker = LapinBroker::new("amqp://localhost:5672").prefetch(nonzero!(64));
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(subscriber("orders", Place).build());
        let reports = RabbitQueue::new("reports").prefetch(nonzero!(1));
        b.include(subscriber(reports, Render).build());
    })
}
// --8<-- [end:prefetch]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
}

// `#[outgoing(name = "orders")]` by hand: the type names the queue it is published to.
impl OutgoingDestination for Order {
    type Form = FixedName;
    const DESTINATION: &'static str = "orders";
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Report {
    id: u64,
}

// `#[outgoing(name = "reports")]` by hand: the type names the queue it is published to.
impl OutgoingDestination for Report {
    type Form = FixedName;
    const DESTINATION: &'static str = "reports";
}

impl MessageHeaders for Report {
    type Contract = NoHeaders;
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::process;

    use ruststream::testing::TestApp;
    use ruststream_lapin::prelude::*;
    use serde::{Deserialize, Serialize};

    use super::{Order, Report, app, live};

    /// Asks the service for `count` reports at once, queued into `queue`.
    #[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
    struct Burst {
        count: u64,
        queue: String,
    }

    /// A report queued for rendering; the publish names the queue.
    #[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
    struct Queued {
        id: u64,
    }

    #[derive(OutSlot)]
    #[publishes(Queued)]
    struct Reports;

    /// Publishes a burst of reports into the windowed queue, all before the first one is settled.
    #[subscriber(RabbitQueue::new("docs.prefetch.bursts").auto_delete(true))]
    async fn burst(request: &Burst, Out(reports): Out<impl Publisher, Reports>) -> HandlerOutcome {
        for id in 0..request.count {
            if reports
                .message(&Queued { id })
                .to(&request.queue)
                .publish()
                .await
                .is_err()
            {
                return HandlerOutcome::retry();
            }
        }
        HandlerOutcome::ack()
    }

    /// A batch handler shows the window: the server never has more than two deliveries out to it.
    #[subscriber(RabbitQueue::new("docs.prefetch.reports")
        .auto_delete(true)
        .prefetch(nonzero!(2)))]
    async fn render_batch(reports: &[Queued]) -> HandlerOutcome {
        tracing::info!(count = reports.len(), "reports rendered");
        HandlerOutcome::ack()
    }

    /// The windowed queue against `uri`, declared by the service under the names of one run, so
    /// two runs on one server never share a queue.
    fn windowed(uri: String, run: &str) -> impl App {
        let bursts = RabbitQueue::new(format!("docs.prefetch.bursts-{run}")).auto_delete(true);
        let reports = RabbitQueue::new(format!("docs.prefetch.reports-{run}"))
            .auto_delete(true)
            .prefetch(nonzero!(2));
        let broker = LapinBroker::new(uri).declare_topology(true);
        RustStream::new(AppInfo::new("reports", "0.1.0")).with_broker(broker, |b| {
            b.include(burst.map_source(|_| bursts))
                .out(Reports, Publish::default())
                .build();
            b.include(render_batch.map_source(|_| reports).batch(nonzero!(10)));
        })
    }

    #[tokio::test]
    async fn both_subscriptions_deliver_under_their_window() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<LapinBroker>()
            .message(&Order { id: 7 })
            .publish()
            .await?;
        tb.broker::<LapinBroker>()
            .message(&Report { id: 1 })
            .publish()
            .await?;

        tb.broker::<LapinBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.broker::<LapinBroker>()
            .subscriber("reports")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_server_holds_back_deliveries_past_the_window() -> Result<(), Box<dyn Error>> {
        let Some(uri) = live::url("AMQP_TEST_URL") else {
            return Ok(());
        };
        let run = process::id().to_string();
        let reports = format!("docs.prefetch.reports-{run}");
        let tb = TestApp::start_live(windowed(uri, &run)).await?;
        tb.broker::<LapinBroker>()
            .message(&Burst {
                count: 5,
                queue: reports.clone(),
            })
            .to(format!("docs.prefetch.bursts-{run}"))
            .publish()
            .await?;

        let batches = tb
            .broker::<LapinBroker>()
            .subscriber(&reports)
            .batches::<Queued>();
        let sizes: Vec<usize> = batches.iter().map(Vec::len).collect();
        assert_eq!(sizes.iter().sum::<usize>(), 5, "{sizes:?}");
        assert!(sizes.iter().all(|&size| size <= 2), "{sizes:?}");
        tb.shutdown().await?;
        Ok(())
    }
}
