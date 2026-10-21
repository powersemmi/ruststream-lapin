//! The service of `examples/lapin_subscriptions_quorum.rs` without the macros: the handler is a named type bound to its descriptor, and `main` is
//! written by hand.
//!
//! ```text
//! cargo test --example manual_lapin_subscriptions_quorum --features testing
//! ```

use std::future::ready;
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;

use serde::{Deserialize, Serialize};

// --8<-- [start:quorum]
use ruststream_lapin::prelude::*;

/// A reservation waits for stock; the queue gives up on one after its delivery limit.
struct Reserve;

impl Handle<Reservation> for Reserve {
    fn handle(
        &self,
        reservation: &Reservation,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        if !reservation.in_stock {
            return ready(Err(HandlerOutcome::retry()));
        }
        tracing::info!(reservation.id, "reserved");
        ready(Ok(()))
    }
}

fn app() -> impl App {
    let broker = LapinBroker::new("amqp://localhost:5672").declare_topology(true);
    RustStream::new(AppInfo::new("reservations", "0.1.0")).with_broker(broker, |b| {
        let reservations = RabbitQuorumQueue::new("reservations");
        b.include(subscriber(reservations, Reserve).build());
    })
}
// --8<-- [end:quorum]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Reservation {
    id: u64,
    in_stock: bool,
}

// `#[outgoing(name = "reservations")]` by hand: the type names the queue it is published to.
impl OutgoingDestination for Reservation {
    type Form = FixedName;
    const DESTINATION: &'static str = "reservations";
}

impl MessageHeaders for Reservation {
    type Contract = NoHeaders;
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_lapin::prelude::*;

    use super::{Reservation, app};

    #[tokio::test]
    async fn a_reservation_in_stock_is_acked() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<LapinBroker>()
            .message(&Reservation {
                id: 7,
                in_stock: true,
            })
            .publish()
            .await?;

        tb.broker::<LapinBroker>()
            .subscriber("reservations")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    // RabbitMQ 4 gives a quorum queue a delivery limit of 20 when it names none: the first
    // delivery and 20 more, then the server takes the message off the queue.
    #[tokio::test]
    async fn the_queue_ends_a_reservation_that_never_fits() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<LapinBroker>()
            .message(&Reservation {
                id: 8,
                in_stock: false,
            })
            .publish()
            .await?;

        tb.broker::<LapinBroker>()
            .subscriber("reservations")
            .assert_called(21)
            .settled(HandlerOutcome::retry());
        tb.shutdown().await?;
        Ok(())
    }
}
