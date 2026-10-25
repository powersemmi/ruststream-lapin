//! The refunds service of the Acknowledgements, retries and dead letters guide: refunds on a
//! classic queue with a waiting queue for delays and a dead-letter queue for declined cards,
//! payouts on a quorum queue that counts its own deliveries, five at most, then a dead-letter
//! queue; written without the macros.
//!
//! ```text
//! docker compose -f docker-compose.test.yml up -d rabbitmq
//! cargo run --example manual_guide_retries --features testing,ruststream/logging -- run
//! ```

use std::convert::Infallible;
use std::future::ready;
use std::process::ExitCode;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;
use tokio::task::yield_now;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_lapin::prelude::*;
use serde::{Deserialize, Serialize};

/// Why the card processor refused a refund.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Refusal {
    /// The processor's rate limit is used up for now.
    Busy,
    /// The processor did not answer.
    Unavailable,
    /// The card account is closed; another try gets the same answer.
    Declined,
}

/// Stands in for the card processor's client.
#[derive(Debug, Default)]
struct Processor {
    /// How many more calls find the processor busy.
    busy_for: Mutex<u32>,
    offline: AtomicBool,
}

impl Processor {
    async fn refund(&self, card: &str) -> Result<(), Refusal> {
        yield_now().await;
        if self.offline.load(Ordering::Relaxed) {
            return Err(Refusal::Unavailable);
        }
        if card.is_empty() {
            return Err(Refusal::Declined);
        }
        let busy = {
            let mut busy_for = self.busy_for.lock().unwrap_or_else(PoisonError::into_inner);
            let busy = *busy_for > 0;
            if busy {
                *busy_for -= 1;
            }
            busy
        };
        if busy { Err(Refusal::Busy) } else { Ok(()) }
    }
}

/// The clients the handlers share, opened once at startup.
#[derive(Debug, Default, Clone)]
struct Clients {
    processor: Arc<Processor>,
}

// --8<-- [start:refund]
/// How long a busy processor asks its callers to wait.
const BACKOFF: Duration = Duration::from_secs(30);

#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Refund {
    id: u64,
    card: String,
    amount_cents: u64,
}

struct ProcessRefund;

impl Handle<Refund, (), (), (), Clients> for ProcessRefund {
    async fn handle(
        &self,
        refund: &Refund,
        _outs: &(),
        ctx: &mut Context<'_, (), Clients>,
    ) -> Result<(), HandlerOutcome> {
        match ctx.state().processor.refund(&refund.card).await {
            Ok(()) => Ok(()),
            // Past the rate limit or silent: a delivery right now would fail again.
            Err(Refusal::Busy | Refusal::Unavailable) => Err(HandlerOutcome::retry_after(BACKOFF)),
            // Another try gets the same answer: the queue moves the refund to its dead letters.
            Err(Refusal::Declined) => {
                tracing::warn!(refund.id, "card declined, refund set aside");
                Err(HandlerOutcome::drop())
            }
        }
    }
}

/// Refunds wait out a delay in the broker's waiting queue, `refunds.retry`.
fn refunds_queue() -> RabbitQueue {
    RabbitQueue::new("refunds").delay(Delay::dlx_ttl())
}
// --8<-- [end:refund]

// --8<-- [start:payout]
#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Payout {
    id: u64,
    card: String,
    amount_cents: u64,
}

struct SendPayout;

impl Handle<Payout, (), (), (), Clients> for SendPayout {
    async fn handle(
        &self,
        payout: &Payout,
        _outs: &(),
        ctx: &mut Context<'_, (), Clients>,
    ) -> Result<(), HandlerOutcome> {
        match ctx.state().processor.refund(&payout.card).await {
            Ok(()) => Ok(()),
            Err(Refusal::Busy | Refusal::Unavailable) => Err(HandlerOutcome::retry()),
            Err(Refusal::Declined) => Err(HandlerOutcome::drop()),
        }
    }
}

/// Payouts live in a quorum queue, which counts their deliveries itself.
fn payouts_queue() -> RabbitQuorumQueue {
    RabbitQuorumQueue::new("payouts")
}
// --8<-- [end:payout]

// --8<-- [start:dead_letters]
struct ReviewRefund;

impl Handle<Refund, (), (), (), Clients> for ReviewRefund {
    fn handle(
        &self,
        refund: &Refund,
        _outs: &(),
        _ctx: &mut Context<'_, (), Clients>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::error!(refund.id, "refund needs a person to look at it");
        ready(Ok(()))
    }
}

struct ReviewPayout;

impl Handle<Payout, (), (), (), Clients> for ReviewPayout {
    fn handle(
        &self,
        payout: &Payout,
        _outs: &(),
        _ctx: &mut Context<'_, (), Clients>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::error!(payout.id, "payout needs a person to look at it");
        ready(Ok(()))
    }
}
// --8<-- [end:dead_letters]

// --8<-- [start:app]
fn service(broker: LapinBroker) -> impl App<State = Clients> {
    RustStream::new(AppInfo::new("refunds", "0.1.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(Clients::default()))
        .with_broker(broker, |b| {
            // The queue moves a dropped refund here: this becomes its dead-letter route.
            b.include(subscriber(refunds_queue(), ProcessRefund).build())
                .dead_letter("refunds.dead");
            b.include(subscriber("refunds.dead", ReviewRefund).build());
            // The server counts these: they become the queue's delivery limit and route.
            b.include(subscriber(payouts_queue(), SendPayout).build())
                .max_attempts(nonzero!(5u32))
                .dead_letter("payouts.dead");
            b.include(subscriber("payouts.dead", ReviewPayout).build());
        })
}

fn app() -> impl App<State = Clients> {
    service(LapinBroker::new("amqp://localhost:5672").declare_topology(true))
}

fn main() -> ExitCode {
    run_main(app)
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::sync::atomic::AtomicBool;
    use std::sync::{Arc, Mutex};

    use ruststream::testing::{Outcome, TestApp};
    use ruststream_lapin::prelude::*;

    use super::{BACKOFF, Clients, Payout, Processor, Refund, app, service};

    fn refund(id: u64) -> Refund {
        Refund {
            id,
            card: "4242".to_owned(),
            amount_cents: 1_500,
        }
    }

    fn with_processor(busy_for: u32, offline: bool) -> Clients {
        Clients {
            processor: Arc::new(Processor {
                busy_for: Mutex::new(busy_for),
                offline: AtomicBool::new(offline),
            }),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_busy_processor_is_asked_again_after_the_backoff() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::with_state(app(), |_| with_processor(1, false)).await?;

        tb.broker::<LapinBroker>()
            .message(&refund(1))
            .to("refunds")
            .publish()
            .await?;
        tb.broker::<LapinBroker>()
            .subscriber("refunds")
            .assert_called_once()
            .settled(HandlerOutcome::retry_after(BACKOFF));

        tb.advance(BACKOFF).await?;

        assert_eq!(
            tb.broker::<LapinBroker>().subscriber("refunds").outcomes(),
            [Outcome::Nack, Outcome::Ack],
        );
        // The waiting queue holds the copy: the service published nothing of its own.
        tb.broker::<LapinBroker>()
            .published::<Refund>("refunds")
            .assert_called_once();
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_declined_card_goes_to_the_dead_letter_queue() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let declined = Refund {
            card: String::new(),
            ..refund(3)
        };

        tb.broker::<LapinBroker>()
            .message(&declined)
            .to("refunds")
            .publish()
            .await?;

        tb.broker::<LapinBroker>()
            .subscriber("refunds")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        tb.broker::<LapinBroker>()
            .subscriber("refunds.dead")
            .assert_called_once()
            .with(&declined);
        // The server moved it: the service published nothing to the dead-letter queue.
        tb.broker::<LapinBroker>()
            .published::<Refund>("refunds.dead")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn the_quorum_queue_moves_a_spent_payout_itself() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::with_state(app(), |_| with_processor(0, true)).await?;
        let payout = Payout {
            id: 4,
            card: "4242".to_owned(),
            amount_cents: 9_000,
        };

        tb.broker::<LapinBroker>()
            .message(&payout)
            .to("payouts")
            .publish()
            .await?;

        tb.broker::<LapinBroker>()
            .subscriber("payouts")
            .assert_called(5);
        tb.broker::<LapinBroker>()
            .subscriber("payouts.dead")
            .assert_called_once()
            .with(&payout);
        // The server moved it: the service published nothing to the dead-letter queue.
        tb.broker::<LapinBroker>()
            .published::<Payout>("payouts.dead")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_quorum_cap_needs_a_service_that_declares_its_queue() {
        let started = TestApp::start(service(LapinBroker::new("amqp://localhost:5672"))).await;

        let refused = started.err().map(|err| err.to_string()).unwrap_or_default();
        assert!(refused.contains("declare_topology(true)"), "{refused}");
    }
}
