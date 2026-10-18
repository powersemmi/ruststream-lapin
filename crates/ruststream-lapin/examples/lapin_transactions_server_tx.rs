//! AMQP channel transactions: both legs of a transfer become visible at commit, or neither does.
//!
//! ```text
//! cargo test --example lapin_transactions_server_tx --features testing
//! cargo run --example lapin_transactions_server_tx -- run
//! ```

use ruststream_lapin::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "transfers")]
struct Transfer {
    from: u64,
    to: u64,
    cents: i64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "ledger.entries")]
struct Entry {
    account: u64,
    cents: i64,
}

/// An account that takes no entries.
const FROZEN: u64 = 13;

// --8<-- [start:ledger]
/// The debit and the credit of a transfer are published in one server transaction. A frozen
/// account rolls it back, and the debit already sent is discarded on the broker.
#[subscriber(RabbitQueue::new("transfers"))]
async fn book(
    transfer: &Transfer,
    Out(ledger): Out<impl TransactionalPublisher>,
) -> HandlerOutcome {
    let Ok(scope) = ledger.begin().await else {
        return HandlerOutcome::retry();
    };
    let legs = [
        (transfer.from, -transfer.cents),
        (transfer.to, transfer.cents),
    ];
    for (account, cents) in legs {
        if account == FROZEN {
            tracing::warn!(account, "frozen account, transfer rolled back");
            return match scope.abort().await {
                Ok(()) => HandlerOutcome::drop(),
                Err(_) => HandlerOutcome::retry(),
            };
        }
        if scope
            .message(&Entry { account, cents })
            .publish()
            .await
            .is_err()
        {
            return HandlerOutcome::retry();
        }
    }
    if scope.commit().await.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("ledger", "0.1.0")).with_broker(
        LapinBroker::new("amqp://localhost:5672"),
        |b| {
            b.include(book)
                .out(DefaultSlot, ServerTxPublish::default())
                .build();
        },
    )
}
// --8<-- [end:ledger]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_lapin::prelude::*;

    use super::{Entry, Transfer, app};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn both_legs_are_published_at_commit() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let transfer = Transfer {
            from: 1,
            to: 2,
            cents: 500,
        };
        tb.broker::<LapinBroker>()
            .message(&transfer)
            .publish()
            .await?;

        let entries = tb
            .broker::<LapinBroker>()
            .published::<Entry>("ledger.entries")
            .assert_called(2)
            .decoded();
        assert_eq!(
            entries,
            [
                Entry {
                    account: 1,
                    cents: -500
                },
                Entry {
                    account: 2,
                    cents: 500
                },
            ]
        );
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_rolled_back_transfer_publishes_no_leg() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let transfer = Transfer {
            from: 1,
            to: 13,
            cents: 500,
        };
        tb.broker::<LapinBroker>()
            .message(&transfer)
            .publish()
            .await?;

        tb.broker::<LapinBroker>()
            .published::<Entry>("ledger.entries")
            .assert_not_called();
        tb.broker::<LapinBroker>()
            .subscriber("transfers")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        tb.shutdown().await?;
        Ok(())
    }
}
