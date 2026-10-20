//! The service of `examples/lapin_exchanges_direct.rs` without the macros: the handlers are named types bound to their descriptors, and `main` is
//! written by hand.
//!
//! ```text
//! cargo test --example manual_lapin_exchanges_direct --features testing
//! ```

use std::future::ready;
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;

use serde::{Deserialize, Serialize};

use ruststream_lapin::prelude::*;

/// The slot payments leave through.
struct Payments;

impl OutSlot for Payments {
    const NAME: &'static str = "Payments";
    // The publish call names the routing key, so a transform on this slot may name it too.
    type Destination = Names;
}

impl PublishedThrough<Payments> for Payment {}

/// Checkout publishes each order's payment to the exchange `payments`, under the payment method
/// as the routing key.
struct Checkout;

impl<Entry> Handle<Order, (), Outs<(Entry,)>> for Checkout
where
    Entry: OutEntry<Payments, Wire: Publisher>,
{
    async fn handle(
        &self,
        order: &Order,
        outs: &Outs<(Entry,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let payment = Payment { order_id: order.id };
        if outs
            .get(Payments)
            .message(&payment)
            .to(&order.method)
            .publish()
            .await
            .is_err()
        {
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
}

// --8<-- [start:direct]
struct ChargeCard;

impl Handle<Payment> for ChargeCard {
    fn handle(
        &self,
        payment: &Payment,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(payment.order_id, "card charged");
        ready(Ok(()))
    }
}

struct SendInvoice;

impl Handle<Payment> for SendInvoice {
    fn handle(
        &self,
        payment: &Payment,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(payment.order_id, "invoice sent");
        ready(Ok(()))
    }
}

fn app() -> impl App {
    let broker = LapinBroker::new("amqp://localhost:5672").declare_topology(true);
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(broker, |b| {
        let payments = RabbitExchange::direct("payments");
        b.include(subscriber("checkout", Checkout).build())
            .out(Payments, Publish::default().exchange("payments"))
            .build();
        let card = RabbitQueue::new("payments.card").bind(payments.clone(), "card");
        b.include(subscriber(card, ChargeCard).build());
        let invoice = RabbitQueue::new("payments.invoice").bind(payments, "invoice");
        b.include(subscriber(invoice, SendInvoice).build());
    })
}
// --8<-- [end:direct]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
    method: String,
}

// `#[outgoing(name = "checkout")]` by hand: the type names the queue it is published to.
impl OutgoingDestination for Order {
    type Form = FixedName;
    const DESTINATION: &'static str = "checkout";
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Payment {
    order_id: u64,
}

// `#[derive(Outgoing)]` by hand: the publish call names the destination.
impl OutgoingDestination for Payment {
    type Form = CallerName;
}

impl MessageHeaders for Payment {
    type Contract = NoHeaders;
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_lapin::prelude::*;

    use super::{Order, Payment, app};

    #[tokio::test]
    async fn a_card_payment_reaches_the_card_queue_alone() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<LapinBroker>()
            .message(&Order {
                id: 7,
                method: "card".to_owned(),
            })
            .publish()
            .await?;

        tb.broker::<LapinBroker>()
            .subscriber("payments.card")
            .assert_called_once()
            .with(&Payment { order_id: 7 });
        tb.broker::<LapinBroker>()
            .subscriber("payments.invoice")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_key_no_queue_is_bound_under_reaches_no_queue() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<LapinBroker>()
            .message(&Order {
                id: 8,
                method: "cash".to_owned(),
            })
            .publish()
            .await?;

        tb.broker::<LapinBroker>()
            .subscriber("checkout")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.broker::<LapinBroker>()
            .subscriber("payments.card")
            .assert_not_called();
        tb.broker::<LapinBroker>()
            .subscriber("payments.invoice")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }
}
