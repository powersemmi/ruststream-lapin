//! The payments service of the Topology guide, written without the macros: each handler is a type
//! with an `impl Handle`, mounted with the `subscriber` constructor, and `main` is written by hand.
//!
//! ```text
//! docker compose -f docker-compose.test.yml up -d rabbitmq
//! cargo run --example manual_guide_topology --features testing,ruststream/logging -- run
//! ```

use std::env;
use std::future::ready;
use std::process::{self, ExitCode};

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_lapin::prelude::*;
use serde::{Deserialize, Serialize};

// --8<-- [start:queues]
#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Payment {
    id: u64,
    card: String,
    amount_cents: u64,
}

struct Charge;

impl Handle<Payment> for Charge {
    fn handle(
        &self,
        payment: &Payment,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        if payment.card.is_empty() {
            tracing::warn!(payment.id, "payment without a card rejected");
            return ready(Err(HandlerOutcome::drop()));
        }
        tracing::info!(payment.id, payment.amount_cents, "payment charged");
        ready(Ok(()))
    }
}

struct RecordFailure;

impl Handle<Payment> for RecordFailure {
    fn handle(
        &self,
        payment: &Payment,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::error!(payment.id, "payment failed");
        ready(Ok(()))
    }
}

/// Payments live in a replicated quorum queue; what it rejects goes to `payments.dead`.
fn payments_queue() -> RabbitQuorumQueue {
    RabbitQuorumQueue::new("payments").dead_letter_exchange("payments.dead")
}

/// Every payment the `payments` queue dead-letters.
fn failed_queue() -> RabbitQueue {
    RabbitQueue::new("payments.failed").bind(RabbitExchange::fanout("payments.dead"), "")
}
// --8<-- [end:queues]

// --8<-- [start:instance]
#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct FxRate {
    currency: String,
    micros_per_euro: u64,
}

/// The name of this instance's own queue.
fn rates_queue_name() -> String {
    format!("payments.rates.{}", process::id())
}

/// A queue of this instance's own: it lives as long as the connection, and a rate older than a
/// minute expires unread.
fn rates_queue() -> RabbitQueue {
    RabbitQueue::new(rates_queue_name())
        .durable(false)
        .exclusive(true)
        .auto_delete(true)
        .argument("x-message-ttl", AMQPValue::LongLongInt(60_000))
        .bind(RabbitExchange::fanout("fx-rates"), "")
}

struct UpdateRate;

impl Handle<FxRate> for UpdateRate {
    fn handle(
        &self,
        rate: &FxRate,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(rate.currency, rate.micros_per_euro, "exchange rate updated");
        ready(Ok(()))
    }
}
// --8<-- [end:instance]

// --8<-- [start:app]
fn service(broker: LapinBroker) -> RustStream {
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(broker, |b| {
        b.include(subscriber(payments_queue(), Charge).build());
        b.include(subscriber(failed_queue(), RecordFailure).build());
        b.include(subscriber(rates_queue(), UpdateRate).build());
    })
}

fn app() -> impl App {
    // Development declares the topology; production runs with `DECLARE_TOPOLOGY=false` against
    // the queues operations created.
    let declare = env::var("DECLARE_TOPOLOGY").map_or(true, |value| value != "false");
    service(LapinBroker::new("amqp://localhost:5672").declare_topology(declare))
}

fn main() -> ExitCode {
    run_main(app)
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_lapin::prelude::*;

    use super::{FxRate, Payment, app, rates_queue_name, service};

    fn payment(id: u64, card: &str) -> Payment {
        Payment {
            id,
            card: card.to_owned(),
            amount_cents: 1_999,
        }
    }

    #[tokio::test]
    async fn a_rejected_payment_reaches_the_failed_queue() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<LapinBroker>()
            .message(&payment(1, ""))
            .to("payments")
            .publish()
            .await?;

        tb.broker::<LapinBroker>()
            .subscriber("payments")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        tb.broker::<LapinBroker>()
            .subscriber("payments.failed")
            .assert_called_once()
            .with(&payment(1, ""));
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn the_instance_queue_takes_rates() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let queue = rates_queue_name();

        tb.broker::<LapinBroker>()
            .message(&FxRate {
                currency: "USD".to_owned(),
                micros_per_euro: 1_084_000,
            })
            .to(&queue)
            .publish()
            .await?;

        tb.broker::<LapinBroker>()
            .subscriber(&queue)
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn the_operated_service_expects_the_same_topology() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(LapinBroker::new("amqp://localhost:5672"))).await?;

        tb.broker::<LapinBroker>()
            .message(&payment(2, ""))
            .to("payments")
            .publish()
            .await?;

        tb.broker::<LapinBroker>()
            .subscriber("payments.failed")
            .assert_called_once()
            .with(&payment(2, ""));
        tb.shutdown().await?;
        Ok(())
    }
}
