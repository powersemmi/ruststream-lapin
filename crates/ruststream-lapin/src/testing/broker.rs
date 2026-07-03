//! The in-process broker: core trait impls plus the `TestableBroker` registration.

use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use ruststream::testing::{Coordinator, TestableBroker};
use ruststream::{Broker, DescribeServer, OutgoingMessage, RawMessage, ServerSpec, Subscribe};

use super::publisher::LapinTestPublisher;
use super::router::KeyRouter;
use super::subscriber::LapinTestSubscriber;
use crate::error::AmqpError;

pub(crate) struct TestBrokerState {
    pub(crate) router: KeyRouter,
    coordinator: OnceLock<Coordinator>,
}

impl TestBrokerState {
    pub(crate) fn install(&self, coordinator: Coordinator) {
        // A second install on the same broker is ignored on purpose: the trait demands
        // idempotency.
        let _ = self.coordinator.set(coordinator);
    }

    pub(crate) fn coordinator(&self) -> Option<Coordinator> {
        self.coordinator.get().cloned()
    }
}

impl Default for TestBrokerState {
    fn default() -> Self {
        Self {
            router: KeyRouter::default(),
            coordinator: OnceLock::new(),
        }
    }
}

impl std::fmt::Debug for TestBrokerState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestBrokerState")
            .field("router", &self.router)
            .finish_non_exhaustive()
    }
}

/// In-process broker for application tests: same descriptors, no `RabbitMQ` server.
///
/// Clones share one router, so a publisher and a subscriber cloned from the same broker see
/// each other; separate [`new`](Self::new) calls are fully isolated.
///
/// # Examples
///
/// ```
/// use ruststream::{Broker, Publisher, Subscriber, OutgoingMessage};
/// use ruststream_lapin::testing::LapinTestBroker;
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), ruststream_lapin::AmqpError> {
/// let broker = LapinTestBroker::new();
/// let mut subscriber = broker.subscribe("orders").await?;
/// broker.publisher().publish(OutgoingMessage::new("orders", b"{}")).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Default)]
pub struct LapinTestBroker {
    state: Arc<TestBrokerState>,
}

impl LapinTestBroker {
    /// Creates an isolated in-process broker.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Subscribes to `queue` (exact-name routing, the default-exchange model).
    ///
    /// # Errors
    ///
    /// Returns [`AmqpError::InvalidOptions`] when `queue` is empty.
    // Async without an await on purpose: call-site parity with the real broker, so application
    // code and tests compile unchanged against either.
    #[allow(clippy::unused_async)]
    pub async fn subscribe(
        &self,
        queue: impl Into<String>,
    ) -> Result<LapinTestSubscriber, AmqpError> {
        let queue = queue.into();
        if queue.is_empty() {
            return Err(AmqpError::InvalidOptions(
                "queue name must not be empty; subscribe with the queue the handler consumes"
                    .to_owned(),
            ));
        }
        Ok(LapinTestSubscriber::open(&self.state, queue))
    }

    /// A publisher into this broker's router.
    #[must_use]
    pub fn publisher(&self) -> LapinTestPublisher {
        LapinTestPublisher::new(Arc::clone(&self.state))
    }
}

impl Broker for LapinTestBroker {
    type Error = AmqpError;

    async fn connect(&self) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), Self::Error> {
        self.state.router.clear();
        Ok(())
    }
}

// `Self::subscribe` inside this impl would resolve to the trait method and recurse; the type
// name is the only way to reach the inherent one.
#[allow(clippy::use_self)]
impl Subscribe for LapinTestBroker {
    type Subscriber = LapinTestSubscriber;

    async fn subscribe(&self, name: &str) -> Result<Self::Subscriber, Self::Error> {
        LapinTestBroker::subscribe(self, name).await
    }
}

impl DescribeServer for LapinTestBroker {
    fn describe_server(&self) -> ServerSpec {
        ServerSpec::in_process("amqp")
    }
}

// --8<-- [start:testable]
impl TestableBroker for LapinTestBroker {
    fn install_coordinator(&self, coordinator: Coordinator) {
        self.state.install(coordinator);
    }

    fn inject(&self, message: OutgoingMessage<'_>) {
        self.state.router.publish(
            message.name(),
            &Bytes::copy_from_slice(message.payload()),
            message.headers(),
            self.state.coordinator().as_ref(),
        );
    }

    fn published(&self, name: &str) -> Vec<RawMessage> {
        self.state.router.published(name)
    }
}

ruststream::register_testable_broker!(LapinTestBroker);
// --8<-- [end:testable]
