//! The service of `examples/lapin_subscriptions_arguments.rs` without the macros: the handler is a named type bound to its descriptor, and `main` is
//! written by hand.
//!
//! ```text
//! cargo test --example manual_lapin_subscriptions_arguments --features testing
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;

use serde::{Deserialize, Serialize};

use ruststream_lapin::prelude::*;

// --8<-- [start:arguments]
struct Notify;

impl Handle<Notification> for Notify {
    fn handle(
        &self,
        notification: &Notification,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(notification.user, "notification sent");
        ready(Ok(()))
    }
}

/// A notification older than a minute is no longer worth sending, and the queue keeps at most
/// ten thousand of them.
fn notifications() -> RabbitQueue {
    let mut arguments = FieldTable::default();
    arguments.insert("x-message-ttl".into(), AMQPValue::LongLongInt(60_000));
    arguments.insert("x-max-length".into(), AMQPValue::LongLongInt(10_000));
    RabbitQueue::new("notifications").arguments(arguments)
}
// --8<-- [end:arguments]

fn app() -> impl App {
    let broker = LapinBroker::new("amqp://localhost:5672").declare_topology(true);
    RustStream::new(AppInfo::new("notifications", "0.1.0")).with_broker(broker, |b| {
        b.include(subscriber(notifications(), Notify).build());
    })
}

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Notification {
    user: u64,
}

// `#[outgoing(name = "notifications")]` by hand: the type names the queue it is published to.
impl OutgoingDestination for Notification {
    type Form = FixedName;
    const DESTINATION: &'static str = "notifications";
}

impl MessageHeaders for Notification {
    type Contract = NoHeaders;
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_lapin::AmqpError;
    use ruststream_lapin::prelude::*;

    use super::{Notification, Notify, app, notifications};

    /// The same queue declared with a shorter time to live.
    fn sooner() -> RabbitQueue {
        RabbitQueue::new("notifications").argument("x-message-ttl", AMQPValue::LongLongInt(1_000))
    }

    #[tokio::test]
    async fn the_declared_queue_takes_a_notification() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<LapinBroker>()
            .message(&Notification { user: 7 })
            .publish()
            .await?;

        tb.broker::<LapinBroker>()
            .subscriber("notifications")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    // A queue keeps the arguments of its first declaration; the server refuses one that asks for
    // others.
    #[tokio::test]
    async fn a_second_declaration_with_other_arguments_fails_startup() {
        let broker = LapinBroker::new("amqp://localhost:5672").declare_topology(true);
        let app =
            RustStream::new(AppInfo::new("notifications", "0.1.0")).with_broker(broker, |b| {
                b.include(subscriber(notifications(), Notify).build());
                b.include(subscriber(sooner(), Notify).build());
            });

        let failed = TestApp::start(app).await.err();
        let mut cause: Option<&(dyn Error + 'static)> = failed.as_ref().map(|err| err as _);
        while let Some(err) = cause {
            if let Some(amqp) = err.downcast_ref::<AmqpError>() {
                assert!(matches!(amqp, AmqpError::Declare(_)), "{amqp}");
                return;
            }
            cause = err.source();
        }
        panic!("startup did not fail with an AmqpError: {failed:?}");
    }
}
