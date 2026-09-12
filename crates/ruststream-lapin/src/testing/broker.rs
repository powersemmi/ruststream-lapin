//! The in-process ladder: [`LapinTestBroker`] -> [`ConnectedLapinTestBroker`].

use std::future::{Future, ready};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use ruststream::testing::{Coordinator, TestableBroker};
use ruststream::{
    Broker, ConnectedBroker, DefaultPublish, DescribeServer, OutgoingMessage, RawMessage,
    ServerSpec, Subscribe,
};

use super::publisher::LapinTestPublishPolicy;
use super::requester::LapinTestRequester;
use super::router::KeyRouter;
use super::subscriber::LapinTestSubscriber;
use crate::error::AmqpError;
use crate::publish_policy::LapinPublish;
use crate::requester::LapinRequest;

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
    /// Numbers the private reply addresses of request/reply. The transport owns the sequence,
    /// like the server that rewrites the direct reply-to address, so two requesters on one broker
    /// cannot be handed the same address.
    inbox_seq: AtomicU64,
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

    pub(crate) fn next_inbox(&self) -> u64 {
        self.inbox_seq.fetch_add(1, Ordering::Relaxed)
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
            inbox_seq: AtomicU64::new(0),
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
/// use ruststream_lapin::LapinPublish;
/// use ruststream_lapin::testing::LapinTestBroker;
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), ruststream_lapin::AmqpError> {
/// let broker = LapinTestBroker::new().connect().await?;
/// let mut subscriber = broker.subscribe("orders").await?;
/// broker
///     .publisher(LapinPublish::default())
///     .publish(OutgoingMessage::new("orders", b"{}"), None)
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

    fn connect(self) -> impl Future<Output = Result<Self::Connected, Self::Error>> {
        ready(Ok(ConnectedLapinTestBroker { state: self.state }))
    }
}

impl DescribeServer for LapinTestBroker {
    fn describe_server(&self) -> ServerSpec {
        ServerSpec::in_process("amqp")
    }
}

/// The connected form of [`LapinTestBroker`].
///
/// Routes published messages by exact queue name (the default-exchange model), hands each
/// delivery to exactly one consumer of that queue as a work queue does, and implements
/// [`TestableBroker`], so it drives both the [`TestApp`](ruststream::testing::TestApp) harness
/// and the framework's conformance suite in process. Clones share one router, so a publisher and
/// a subscriber taken from the same broker see each other.
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
    // Awaitable although nothing here awaits: call-site parity with the real broker, so
    // application code and tests compile unchanged against either.
    pub fn subscribe(
        &self,
        queue: impl Into<String>,
    ) -> impl Future<Output = Result<LapinTestSubscriber, AmqpError>> {
        let queue = queue.into();
        if queue.is_empty() {
            return ready(Err(AmqpError::InvalidOptions(
                "queue name must not be empty; subscribe with the queue the handler consumes"
                    .to_owned(),
            )));
        }
        if let Err(err) = self.state.ensure_live(&queue) {
            return ready(Err(err));
        }
        ready(Ok(LapinTestSubscriber::open(&self.state, queue)))
    }

    /// A live publisher into this broker's router, mirroring
    /// [`ConnectedLapinBroker::publisher`](crate::ConnectedLapinBroker::publisher).
    ///
    /// It takes the crate's production policies, and each of them pairs into the stand-in for the
    /// publisher it produces on a server, carrying the same capabilities: a routes file written
    /// for `RabbitMQ` mounts here unchanged, and a mount a server would reject does not compile
    /// here either.
    #[must_use]
    pub fn publisher<P: LapinTestPublishPolicy>(&self, policy: P) -> P::Live {
        policy.bind(self)
    }

    /// A live request/reply client over the transport's own reply addressing.
    ///
    /// The requester half of [`LapinRequest`]; [`publisher`](Self::publisher) accepts the same
    /// policy, this accessor only names the result.
    #[must_use]
    pub fn requester(&self, policy: LapinRequest) -> LapinTestRequester {
        policy.bind(self)
    }
}

impl ConnectedBroker for ConnectedLapinTestBroker {
    type Error = AmqpError;
    type Closed = ();

    fn shutdown(self) -> impl Future<Output = Result<Self::Closed, Self::Error>> {
        self.state.closed.store(true, Ordering::Release);
        self.state.router.clear();
        ready(Ok(()))
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

/// The same default the real broker carries, so a `publish(..)` handler mounted without an
/// explicit publisher replies through the same policy in both places.
impl DefaultPublish for ConnectedLapinTestBroker {
    type Policy = LapinPublish;
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
