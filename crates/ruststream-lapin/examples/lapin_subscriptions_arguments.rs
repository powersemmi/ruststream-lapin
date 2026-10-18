//! `argument` and `arguments`: queue declaration arguments the descriptor does not model, passed
//! to the server verbatim.
//!
//! ```text
//! cargo test --example lapin_subscriptions_arguments --features testing
//! ```

use serde::{Deserialize, Serialize};

use ruststream_lapin::prelude::*;

// --8<-- [start:arguments]
/// A notification older than a minute is no longer worth sending, and the queue keeps at most
/// ten thousand of them.
#[subscriber(RabbitQueue::new("notifications")
    .argument("x-message-ttl", AMQPValue::LongLongInt(60_000))
    .argument("x-max-length", AMQPValue::LongLongInt(10_000)))]
async fn notify(notification: &Notification) -> HandlerOutcome {
    tracing::info!(notification.user, "notification sent");
    HandlerOutcome::ack()
}
// --8<-- [end:arguments]

#[ruststream::app]
fn app() -> impl App {
    let broker = LapinBroker::new("amqp://localhost:5672").declare_topology(true);
    RustStream::new(AppInfo::new("notifications", "0.1.0")).with_broker(broker, |b| {
        b.include(notify);
    })
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "notifications")]
struct Notification {
    user: u64,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_lapin::AmqpError;
    use ruststream_lapin::prelude::*;

    use super::{Notification, app, notify};

    /// The same queue declared with a shorter time to live.
    #[subscriber(RabbitQueue::new("notifications")
        .argument("x-message-ttl", AMQPValue::LongLongInt(1_000)))]
    async fn notify_sooner(notification: &Notification) -> HandlerOutcome {
        tracing::info!(notification.user, "notification sent");
        HandlerOutcome::ack()
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
                b.include(notify);
                b.include(notify_sooner);
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
