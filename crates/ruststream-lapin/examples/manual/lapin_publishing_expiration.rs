//! The service of `examples/lapin_publishing_expiration.rs` written without `#[subscriber]`: the
//! body implements `Handle` over a slot whose publisher takes this crate's options.
//!
//! ```text
//! cargo test --example manual_lapin_publishing_expiration --features testing
//! cargo run --example manual_lapin_publishing_expiration -- run
//! ```

use std::process::ExitCode;
use std::time::Duration;

use ruststream::OutSlot;
use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_lapin::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "prices")]
struct PriceChange {
    sku: String,
    cents: u64,
    flash: bool,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "quotes")]
struct Quote {
    sku: String,
    cents: u64,
}

// --8<-- [start:expiration]
#[derive(OutSlot)]
#[publishes(Quote)]
struct Quotes;

/// A quote is worth a minute; a flash quote, ten seconds. The broker drops a quote nobody
/// consumed in time.
struct PublishQuote;

impl<Egress> Handle<PriceChange, (), Outs<(Egress,)>> for PublishQuote
where
    Egress: OutEntry<Quotes, Wire: Publisher<Options = LapinPublishOptions>>,
{
    async fn handle(
        &self,
        change: &PriceChange,
        outs: &Outs<(Egress,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let quote = Quote {
            sku: change.sku.clone(),
            cents: change.cents,
        };
        let quotes = outs.get(Quotes);
        let sent = if change.flash {
            quotes
                .message(&quote)
                .expiration(Duration::from_secs(10))
                .publish()
                .await
        } else {
            quotes.message(&quote).publish().await
        };
        if sent.is_err() {
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("pricing", "0.1.0")).with_broker(
        LapinBroker::new("amqp://localhost:5672"),
        |b| {
            b.include(subscriber(RabbitQueue::new("prices"), PublishQuote).build())
                .out(
                    Quotes,
                    Publish::default().expiration(Duration::from_mins(1)),
                )
                .build();
        },
    )
}
// --8<-- [end:expiration]

fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::Duration;

    use ruststream::testing::TestApp;
    use ruststream_lapin::prelude::*;

    use super::{PriceChange, Quotes, app};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_flash_quote_expires_sooner() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<LapinBroker>()
            .message(&PriceChange {
                sku: "tea".into(),
                cents: 450,
                flash: true,
            })
            .publish()
            .await?;

        tb.out::<Quotes>()
            .assert_called_once()
            .with_options(&LapinPublishOptions {
                expiration: Some(Duration::from_secs(10)),
                ..LapinPublishOptions::default()
            });
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_ordinary_quote_takes_the_policy_ttl() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<LapinBroker>()
            .message(&PriceChange {
                sku: "tea".into(),
                cents: 450,
                flash: false,
            })
            .publish()
            .await?;

        tb.out::<Quotes>()
            .assert_called_once()
            .assert_options_default();
        tb.shutdown().await?;
        Ok(())
    }
}
