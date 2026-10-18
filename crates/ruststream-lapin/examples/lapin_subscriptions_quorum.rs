//! `RabbitQuorumQueue`: a replicated queue that counts the deliveries each message spends and
//! ends a retry loop itself.
//!
//! ```text
//! cargo test --example lapin_subscriptions_quorum --features testing
//! ```

use serde::{Deserialize, Serialize};

// --8<-- [start:quorum]
use ruststream_lapin::prelude::*;

/// A reservation waits for stock; the queue gives up on one after its delivery limit.
#[subscriber(RabbitQuorumQueue::new("reservations"))]
async fn reserve(reservation: &Reservation) -> HandlerOutcome {
    if !reservation.in_stock {
        return HandlerOutcome::retry();
    }
    tracing::info!(reservation.id, "reserved");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    let broker = LapinBroker::new("amqp://localhost:5672").declare_topology(true);
    RustStream::new(AppInfo::new("reservations", "0.1.0")).with_broker(broker, |b| {
        b.include(reserve);
    })
}
// --8<-- [end:quorum]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "reservations")]
struct Reservation {
    id: u64,
    in_stock: bool,
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
