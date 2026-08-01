//! The in-process ladder: [`LapinTestBroker`] -> [`ConnectedLapinTestBroker`].

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use ruststream::testing::{Coordinator, TestableBroker};
use ruststream::{
    Broker, ConnectedBroker, DefaultPublish, DescribeServer, OutgoingMessage, RawMessage,
    ServerSpec, Subscribe,
};

use super::publisher::{LapinTestPublish, LapinTestPublisher};
use super::router::KeyRouter;
use super::subscriber::LapinTestSubscriber;
use crate::error::AmqpError;

/// Shared state owned by every handle on a single test broker instance.
///
/// The unconnected broker, its connected form, and every publisher paired off it share one
/// [`Arc`] of this, so they all see the same router. Distinct instances (different
/// [`LapinTestBroker::new`] calls) are fully isolated.
pub(crate) struct TestBrokerState {
    pub(crate) router: KeyRouter,
    /// Mirrors the real broker's post-shutdown behaviour: handles aliasing a shut-down transport
    /// must report an error rather than route into a dead router.
    closed: AtomicBool,
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

    /// `Ok` while the transport is live, [`AmqpError::Closed`] once it has shut down.
    pub(crate) fn ensure_live(&self, target: &str) -> Result<(), AmqpError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(AmqpError::closed(target));
        }
        Ok(())
    }
}

impl Default for TestBrokerState {
    fn default() -> Self {
        Self {
            router: KeyRouter::default(),
            closed: AtomicBool::new(false),
            coordinator: OnceLock::new(),
        }
    }
}

impl std::fmt::Debug for TestBrokerState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestBrokerState")
            .field("router", &self.router)
            .field("closed", &self.closed.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

/// In-process broker for application tests: same descriptors, no `RabbitMQ` server.
///
/// Mirrors the real ladder: `new` is synchronous, and the consuming `connect` hands out the
/// [`ConnectedLapinTestBroker`] that carries the subscribe and publish surface.
///
/// # Examples
///
/// ```
/// use ruststream::{Broker, OutgoingMessage, Publisher, Subscriber};
/// use ruststream_lapin::testing::{LapinTestBroker, LapinTestPublish};
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), ruststream_lapin::AmqpError> {
/// let broker = LapinTestBroker::new().connect().await?;
/// let mut subscriber = broker.subscribe("orders").await?;
/// broker
///     .publisher(LapinTestPublish)
///     .publish(OutgoingMessage::new("orders", b"{}"))
///     .await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Default)]
#[must_use]
pub struct LapinTestBroker {
    state: Arc<TestBrokerState>,
}

impl LapinTestBroker {
    /// Creates an isolated in-process broker.
    pub fn new() -> Self {
        Self::default()
    }
}

impl Broker for LapinTestBroker {
    type Error = AmqpError;
    type Connected = ConnectedLapinTestBroker;

    async fn connect(self) -> Result<Self::Connected, Self::Error> {
        Ok(ConnectedLapinTestBroker { state: self.state })
    }
}

impl DescribeServer for LapinTestBroker {
    fn describe_server(&self) -> ServerSpec {
        ServerSpec::in_process("amqp")
    }
}

/// The connected form of [`LapinTestBroker`].
///
/// Routes published messages to subscribers by exact queue name (the default-exchange model) and
/// implements [`TestableBroker`], so it drives both the
/// [`TestApp`](ruststream::testing::TestApp) harness and the framework's conformance suite in
/// process. Clones share one router, so a publisher and a subscriber taken from the same broker
/// see each other.
#[derive(Debug, Clone)]
pub struct ConnectedLapinTestBroker {
    state: Arc<TestBrokerState>,
}

impl ConnectedLapinTestBroker {
    pub(crate) fn state(&self) -> Arc<TestBrokerState> {
        Arc::clone(&self.state)
    }

    /// Subscribes to `queue` (exact-name routing, the default-exchange model).
    ///
    /// # Errors
    ///
    /// Returns [`AmqpError::InvalidOptions`] when `queue` is empty and [`AmqpError::Closed`]
    /// once the transport has shut down.
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
        self.state.ensure_live(&queue)?;
        Ok(LapinTestSubscriber::open(&self.state, queue))
    }

    /// A live publisher into this broker's router, mirroring
    /// [`ConnectedLapinBroker::publisher`](crate::ConnectedLapinBroker::publisher). The
    /// in-process transport routes by queue name only, so it has a single policy.
    #[must_use]
    pub fn publisher(&self, policy: LapinTestPublish) -> LapinTestPublisher {
        policy.bind(self)
    }
}

impl ConnectedBroker for ConnectedLapinTestBroker {
    type Error = AmqpError;
    type Closed = ();

    async fn shutdown(self) -> Result<Self::Closed, Self::Error> {
        self.state.closed.store(true, Ordering::Release);
        self.state.router.clear();
        Ok(())
    }
}

// `Self::subscribe` inside this impl would resolve to the trait method and recurse; the type
// name is the only way to reach the inherent one.
#[allow(clippy::use_self)]
impl Subscribe for ConnectedLapinTestBroker {
    type Subscriber = LapinTestSubscriber;

    async fn subscribe(&self, name: &str) -> Result<Self::Subscriber, Self::Error> {
        ConnectedLapinTestBroker::subscribe(self, name).await
    }
}

impl DefaultPublish for ConnectedLapinTestBroker {
    type Policy = LapinTestPublish;
}

// --8<-- [start:testable]
impl TestableBroker for ConnectedLapinTestBroker {
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

ruststream::register_testable_broker!(ConnectedLapinTestBroker);
// --8<-- [end:testable]
