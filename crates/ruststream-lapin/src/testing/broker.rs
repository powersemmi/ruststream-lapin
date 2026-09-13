//! The in-process ladder: [`LapinTestBroker`] -> [`ConnectedLapinTestBroker`].

use std::collections::HashMap;
use std::future::{Future, ready};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use bytes::Bytes;
use lapin::types::FieldTable;
use ruststream::testing::{Coordinator, TestableBroker};
use ruststream::{
    AddressedCopies, Broker, ConnectedBroker, DefaultPublish, DescribeServer, OutgoingMessage,
    RawMessage, ServerSpec, Subscribe,
};

use super::publisher::LapinTestPublishPolicy;
use super::requester::LapinTestRequester;
use super::router::KeyRouter;
use super::subscriber::{LapinTestSubscriber, QueueBehaviour};
use crate::error::AmqpError;
use crate::publish_policy::LapinPublish;
use crate::queue::{QueueDescriptor, declared_arguments};
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
    /// The arguments each descriptor's declaration produced, by queue name. The transport
    /// declares nothing, but a test reads here what a server would have been asked for.
    declared: Mutex<HashMap<String, FieldTable>>,
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

    /// Records what a subscription's declaration asked the queue for.
    pub(crate) fn declare(&self, queue: &str, arguments: FieldTable) {
        self.declared
            .lock()
            .expect("declared-arguments mutex poisoned")
            .insert(queue.to_owned(), arguments);
    }

    /// What the declaration of `queue` asked for, or `None` for a queue nothing subscribed to.
    pub(crate) fn declared(&self, queue: &str) -> Option<FieldTable> {
        self.declared
            .lock()
            .expect("declared-arguments mutex poisoned")
            .get(queue)
            .cloned()
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
            declared: Mutex::new(HashMap::new()),
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

    /// The queue arguments the descriptor mounted on `queue` was declared with, or `None` for a
    /// queue nothing has subscribed to.
    ///
    /// The same answer [`ConnectedLapinTestBroker::declared_arguments`] gives, reachable from a
    /// clone kept before the app took the broker - which is how a
    /// [`TestApp`](ruststream::testing::TestApp) test reads what a registration asked its queue
    /// for.
    ///
    /// # Panics
    ///
    /// Panics if another thread panicked while recording a declaration.
    #[must_use]
    pub fn declared_arguments(&self, queue: &str) -> Option<FieldTable> {
        self.state.declared(queue)
    }
}

impl Broker for LapinTestBroker {
    type Error = AmqpError;
    type Connected = ConnectedLapinTestBroker;

    fn connect(self) -> impl Future<Output = Result<Self::Connected, Self::Error>> {
        ready(Ok(ConnectedLapinTestBroker { state: self.state }))
    }
}

/// The same protocol and version the real broker reports, so a document generated in a test is
/// the document the service publishes.
impl DescribeServer for LapinTestBroker {
    fn describe_server(&self) -> ServerSpec {
        ServerSpec::in_process(crate::broker::PROTOCOL)
            .protocol_version(crate::broker::PROTOCOL_VERSION)
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
        self.open(queue.into(), QueueBehaviour::default())
    }

    /// Subscribes for `def`, carrying what the transport can honour of it beyond the queue name:
    /// whether a delayed redelivery is the broker's, and whether the queue counts the deliveries a
    /// message spends and carries a spent one away itself.
    ///
    /// The rest of the descriptor is topology the in-process transport has none of, so it records
    /// the arguments the declaration produced and answers with them from
    /// [`declared_arguments`](Self::declared_arguments) instead. A declaration the queue cannot
    /// carry is refused here exactly as a server refuses it.
    pub(crate) async fn subscribe_to(
        &self,
        def: &(impl QueueDescriptor + Sync),
    ) -> Result<LapinTestSubscriber, AmqpError> {
        let spec = def.spec();
        // The stand declares every queue it opens, which is the case where a declaration reaches
        // the queue at all.
        def.check_retry(true)?;
        let arguments = declared_arguments(spec, def.declared_retry());
        let behaviour = QueueBehaviour::of(spec, &arguments);
        self.state.declare(&spec.name, arguments);
        self.open(spec.name.clone(), behaviour).await
    }

    /// The queue arguments the descriptor mounted on `queue` was declared with, or `None` for a
    /// queue nothing has subscribed to.
    ///
    /// This is what a server would have been asked for: the queue type, and on a quorum queue the
    /// delivery limit and dead-letter route the mount site's `max_attempts(..).dead_letter(..)`
    /// turned into.
    ///
    /// # Panics
    ///
    /// Panics if another thread panicked while recording a declaration.
    #[must_use]
    pub fn declared_arguments(&self, queue: &str) -> Option<FieldTable> {
        self.state.declared(queue)
    }

    fn open(
        &self,
        queue: String,
        behaviour: QueueBehaviour,
    ) -> impl Future<Output = Result<LapinTestSubscriber, AmqpError>> {
        if queue.is_empty() {
            return ready(Err(AmqpError::InvalidOptions(
                "queue name must not be empty; subscribe with the queue the handler consumes"
                    .to_owned(),
            )));
        }
        if let Err(err) = self.state.ensure_live(&queue) {
            return ready(Err(err));
        }
        ready(Ok(LapinTestSubscriber::open(&self.state, queue, behaviour)))
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

    /// The copy path the real broker takes: a queue name is where a publish reaches the
    /// subscription opened under it. Declaring anything else here would let an app that binds
    /// `out_retry` over `#[subscriber("orders")]` compile in a test and not on a server.
    type Copies = AddressedCopies;

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
