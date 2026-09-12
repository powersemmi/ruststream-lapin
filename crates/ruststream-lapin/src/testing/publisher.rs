//! The in-process publish half: the crate's production policies paired against the test broker,
//! and the live publishers they produce.
//!
//! There is one stand-in per production publisher, carrying exactly the capabilities its
//! counterpart carries. That parity is the point of the whole module: a routes file mounts on the
//! test broker unchanged, and a mount the real broker would reject does not compile here either.
//! [`LapinPublish`] pairs into a publisher with no transaction surface at all, which is the
//! compile-time check [`confirms`](LapinPublish::confirms) exists for;
//! [`ConfirmsPublish`] carries both transaction shapes and [`ServerTxPublish`] only the borrowed
//! one, exactly as their live counterparts do.
//!
//! What the transport underneath cannot reproduce is stated per publisher below, and collected in
//! the crate's testing guide.

use std::future::{Future, ready};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use ruststream::{
    HeaderMap, OutgoingMessage, OwnedTransactions, PairError, PublishPolicy, Publisher,
    Transaction, TransactionalPublisher,
};
use tracing::warn;

use super::broker::{ConnectedLapinTestBroker, TestBrokerState};
use crate::error::AmqpError;
use crate::publish_policy::sealed::Sealed;
use crate::publish_policy::{ConfirmsPublish, LapinPublish, ServerTxPublish};
use crate::publisher::Buffered;

/// A publish policy that pairs with the connected test broker, the in-process counterpart of
/// [`LapinPublishPolicy`](crate::LapinPublishPolicy).
///
/// The implementors are the crate's production policies themselves, so the mount site of a test
/// app is spelled exactly like the mount site it stands in for. Pairing allocates and never
/// awaits, which is what lets
/// [`ConnectedLapinTestBroker::publisher`](super::ConnectedLapinTestBroker::publisher) be
/// synchronous; [`PublishPolicy::pair`], the framework-side entry point, delegates here.
///
/// # Examples
///
/// ```
/// use ruststream::Broker;
/// use ruststream_lapin::LapinPublish;
/// use ruststream_lapin::testing::{LapinTestBroker, LapinTestPublishPolicy};
///
/// # async fn demo() -> Result<(), ruststream_lapin::AmqpError> {
/// let broker = LapinTestBroker::new().connect().await?;
/// let publisher = LapinPublish::default().bind(&broker);
/// # let _ = publisher;
/// # Ok(())
/// # }
/// ```
pub trait LapinTestPublishPolicy: PublishPolicy<ConnectedLapinTestBroker> + Sealed {
    /// Pairs the policy with the connected test broker, producing the live publisher.
    #[must_use]
    fn bind(self, connected: &ConnectedLapinTestBroker) -> Self::Live;
}

/// The transport handle every in-process publisher holds: the router plus the checks a live
/// publisher makes before the broker would.
#[derive(Debug, Clone)]
pub(super) struct Routed {
    state: Arc<TestBrokerState>,
}

impl Routed {
    pub(super) fn new(connected: &ConnectedLapinTestBroker) -> Self {
        Self {
            state: connected.state(),
        }
    }

    pub(super) fn state(&self) -> &Arc<TestBrokerState> {
        &self.state
    }

    /// Routes `msg` to the queue its name addresses.
    pub(super) fn send(&self, msg: &OutgoingMessage<'_>) -> Result<(), AmqpError> {
        self.send_parts(
            msg.name(),
            &Bytes::copy_from_slice(msg.payload()),
            msg.headers(),
        )
    }

    /// The same fanout for a buffered message, which is already split into its parts.
    fn send_buffered(&self, entry: &Buffered) -> Result<(), AmqpError> {
        self.send_parts(&entry.routing_key, &entry.payload, &entry.headers)
    }

    fn send_parts(
        &self,
        routing_key: &str,
        payload: &Bytes,
        headers: &HeaderMap,
    ) -> Result<(), AmqpError> {
        self.check(routing_key)?;
        self.state.router.publish(
            routing_key,
            payload,
            headers,
            self.state.coordinator().as_ref(),
        );
        Ok(())
    }

    /// The two failures a live publisher reports before the message reaches the transport: an
    /// unusable routing key, and a handle outliving the connection.
    fn check(&self, routing_key: &str) -> Result<(), AmqpError> {
        if routing_key.is_empty() {
            return Err(AmqpError::InvalidOptions(
                "routing key must not be empty; on the default exchange it names the target queue"
                    .to_owned(),
            ));
        }
        self.state.ensure_live(routing_key)
    }
}

/// The client-side transaction buffer both transactional stand-ins keep.
///
/// Publishes made while a transaction is open accumulate here and reach the router in publish
/// order at commit; an abort drops them. The publisher naming itself in the diagnostics is passed
/// per call, so this carries no field for it.
#[derive(Debug, Clone)]
struct Buffering {
    route: Routed,
    txn: Arc<Mutex<Option<Vec<Buffered>>>>,
}

impl Buffering {
    fn new(connected: &ConnectedLapinTestBroker) -> Self {
        Self {
            route: Routed::new(connected),
            txn: Arc::new(Mutex::new(None)),
        }
    }

    /// Buffers `msg` inside an open transaction, or routes it straight away.
    fn publish(&self, msg: &OutgoingMessage<'_>) -> Result<(), AmqpError> {
        // Checked before buffering, not only at the flush: the live publishers reject an
        // unusable routing key and a dead connection at the call that made the mistake.
        self.route.check(msg.name())?;
        {
            let mut txn = self.txn.lock().expect("transaction buffer mutex poisoned");
            if let Some(buffer) = txn.as_mut() {
                buffer.push(Buffered::new(msg));
                return Ok(());
            }
        }
        self.route.send(msg)
    }

    fn begin(&self, publisher: &str) -> Result<(), AmqpError> {
        let already_open = {
            let mut txn = self.txn.lock().expect("transaction buffer mutex poisoned");
            let open = txn.is_some();
            if !open {
                *txn = Some(Vec::new());
            }
            open
        };
        if already_open {
            return Err(AmqpError::Transaction(format!(
                "a transaction is already open on this {publisher}; commit or abort it before \
                 beginning another"
            )));
        }
        Ok(())
    }

    fn commit(&self, publisher: &str) -> Result<(), AmqpError> {
        let buffered = {
            let mut txn = self.txn.lock().expect("transaction buffer mutex poisoned");
            txn.take()
        };
        let Some(buffered) = buffered else {
            return Err(AmqpError::Transaction(format!(
                "commit with no open transaction on this {publisher}"
            )));
        };
        for entry in &buffered {
            self.route.send_buffered(entry)?;
        }
        Ok(())
    }

    fn abort(&self, publisher: &str) -> Result<(), AmqpError> {
        let discarded = self
            .txn
            .lock()
            .expect("transaction buffer mutex poisoned")
            .take();
        if discarded.is_none() {
            return Err(AmqpError::Transaction(format!(
                "abort with no open transaction on this {publisher}"
            )));
        }
        Ok(())
    }
}

/// The in-process stand-in for [`LapinPublisher`](crate::LapinPublisher): fire-and-forget
/// publishing into the router, and nothing else.
///
/// Paired from [`LapinPublish`], so a mount site keeps the production policy. It carries no
/// transaction surface, exactly like its counterpart: an `Out<impl TransactionalPublisher>`
/// handler mounted on this policy fails to compile here as it does on a real server, and
/// [`confirms`](LapinPublish::confirms) is the transition that fixes it in both places.
///
/// The policy's exchange and persistence are inert: the transport routes by exact queue name and
/// has nothing to persist to. Like the real publishers it aliases the transport and may outlive
/// it, so after the broker shuts down every publish reports [`AmqpError::Closed`].
///
/// # Examples
///
/// ```
/// use ruststream::{Broker, OutgoingMessage, Publisher};
/// use ruststream_lapin::LapinPublish;
/// use ruststream_lapin::testing::LapinTestBroker;
///
/// # async fn demo() -> Result<(), ruststream_lapin::AmqpError> {
/// let broker = LapinTestBroker::new().connect().await?;
/// let publisher = broker.publisher(LapinPublish::default());
/// publisher
///     .publish(OutgoingMessage::new("orders", b"{}".as_slice()))
///     .await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct LapinTestPublisher {
    route: Routed,
}

impl Publisher for LapinTestPublisher {
    type Error = AmqpError;

    /// Routes `msg` to the subscribers of the queue named by `msg.name()`.
    ///
    /// # Errors
    ///
    /// Returns [`AmqpError::InvalidOptions`] when the routing key is empty and
    /// [`AmqpError::Closed`] once the transport has shut down.
    fn publish(&self, msg: OutgoingMessage<'_>) -> impl Future<Output = Result<(), Self::Error>> {
        ready(self.route.send(&msg))
    }
}

impl PublishPolicy<ConnectedLapinTestBroker> for LapinPublish {
    type Live = LapinTestPublisher;

    fn pair(
        self,
        connected: &ConnectedLapinTestBroker,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(LapinTestPublishPolicy::bind(self, connected)))
    }
}

impl LapinTestPublishPolicy for LapinPublish {
    fn bind(self, connected: &ConnectedLapinTestBroker) -> Self::Live {
        LapinTestPublisher {
            route: Routed::new(connected),
        }
    }
}

/// The in-process stand-in for [`ConfirmsPublisher`](crate::ConfirmsPublisher).
///
/// It carries both transaction shapes, as its counterpart does: the handle-level borrowed one
/// ([`TransactionalPublisher`]) and the owned [`ConfirmsTestTransaction`]
/// ([`OwnedTransactions`]).
///
/// Paired from [`ConfirmsPublish`]. Publisher confirms are a client-side buffer on a real server
/// too, so what a handler observes is reproduced exactly: publishes made inside a transaction
/// reach the router in publish order at commit, an abort discards them, and a settle call with no
/// open transaction errors. Clones share the handle-level buffer.
///
/// What is not reproduced is the confirm itself. On a server every message is acknowledged
/// individually and a commit fails when the broker nacks one; in process the flush cannot fail
/// that way, so a test must not read a successful commit as evidence that the broker accepted the
/// batch. The negative-confirm path is covered against a live server by the crate's integration
/// tests.
///
/// # Examples
///
/// ```
/// use ruststream::{Broker, OutgoingMessage, Publisher, TransactionalPublisher};
/// use ruststream_lapin::LapinPublish;
/// use ruststream_lapin::testing::LapinTestBroker;
///
/// # async fn demo() -> Result<(), ruststream_lapin::AmqpError> {
/// let broker = LapinTestBroker::new().connect().await?;
/// let publisher = broker.publisher(LapinPublish::default().confirms());
///
/// publisher.begin_transaction().await?;
/// publisher
///     .publish(OutgoingMessage::new("orders", b"{}".as_slice()))
///     .await?;
/// publisher.commit().await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct ConfirmsTestPublisher {
    buffering: Buffering,
}

/// How the borrowed-transaction diagnostics name this publisher.
const CONFIRMS: &str = "confirms test publisher";

impl Publisher for ConfirmsTestPublisher {
    type Error = AmqpError;

    /// Routes `msg`, or buffers it when a transaction is open on this handle.
    ///
    /// # Errors
    ///
    /// Returns [`AmqpError::InvalidOptions`] when the routing key is empty and
    /// [`AmqpError::Closed`] once the transport has shut down.
    fn publish(&self, msg: OutgoingMessage<'_>) -> impl Future<Output = Result<(), Self::Error>> {
        ready(self.buffering.publish(&msg))
    }
}

impl TransactionalPublisher for ConfirmsTestPublisher {
    /// Opens the handle-level buffering transaction.
    ///
    /// # Errors
    ///
    /// Returns [`AmqpError::Transaction`] when a transaction is already open; the open
    /// transaction is left untouched.
    fn begin_transaction(&self) -> impl Future<Output = Result<(), Self::Error>> {
        ready(self.buffering.begin(CONFIRMS))
    }

    /// Routes the buffered messages in publish order.
    ///
    /// # Errors
    ///
    /// Returns [`AmqpError::Transaction`] when no transaction is open and [`AmqpError::Closed`]
    /// once the transport has shut down. Like the live publisher, messages already flushed stay
    /// published: confirms are per message, not atomic across them.
    fn commit(&self) -> impl Future<Output = Result<(), Self::Error>> {
        ready(self.buffering.commit(CONFIRMS))
    }

    /// Discards the buffered messages.
    ///
    /// # Errors
    ///
    /// Returns [`AmqpError::Transaction`] when no transaction is open.
    fn abort(&self) -> impl Future<Output = Result<(), Self::Error>> {
        ready(self.buffering.abort(CONFIRMS))
    }
}

/// Owned transactions, mirroring [`ConfirmsPublisher`](crate::ConfirmsPublisher): every call
/// opens an independent buffer-owning [`ConfirmsTestTransaction`], so any number can be open at
/// once and the publisher keeps routing directly meanwhile.
impl OwnedTransactions for ConfirmsTestPublisher {
    type Transaction = ConfirmsTestTransaction;

    /// Opens a transaction owned by the returned value.
    ///
    /// # Errors
    ///
    /// Never fails: opening allocates a buffer and never touches the router.
    fn transaction(&self) -> impl Future<Output = Result<Self::Transaction, Self::Error>> {
        ready(Ok(ConfirmsTestTransaction {
            route: self.buffering.route.clone(),
            buffered: Vec::new(),
            settled: false,
        }))
    }
}

impl PublishPolicy<ConnectedLapinTestBroker> for ConfirmsPublish {
    type Live = ConfirmsTestPublisher;

    fn pair(
        self,
        connected: &ConnectedLapinTestBroker,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(LapinTestPublishPolicy::bind(self, connected)))
    }
}

impl LapinTestPublishPolicy for ConfirmsPublish {
    fn bind(self, connected: &ConnectedLapinTestBroker) -> Self::Live {
        ConfirmsTestPublisher {
            buffering: Buffering::new(connected),
        }
    }
}

/// An owned in-process transaction, opened by
/// [`transaction`](OwnedTransactions::transaction) on a [`ConfirmsTestPublisher`].
///
/// A private buffer routed in publish order on commit and discarded on abort, mirroring
/// [`ConfirmsTransaction`](crate::ConfirmsTransaction). Both settle by consuming `self`, so a
/// double commit or a publish after settling is a compile error.
///
/// # Examples
///
/// ```
/// use ruststream::{Broker, OutgoingMessage, OwnedTransactions, Transaction};
/// use ruststream_lapin::LapinPublish;
/// use ruststream_lapin::testing::LapinTestBroker;
///
/// # async fn demo() -> Result<(), ruststream_lapin::AmqpError> {
/// let broker = LapinTestBroker::new().connect().await?;
/// let mut txn = broker
///     .publisher(LapinPublish::default().confirms())
///     .transaction()
///     .await?;
/// txn.publish(OutgoingMessage::new("orders", b"{}".as_slice()))
///     .await?;
/// txn.commit().await?;
/// # Ok(())
/// # }
/// ```
#[must_use = "a transaction does nothing until settled with commit() or abort()"]
pub struct ConfirmsTestTransaction {
    route: Routed,
    buffered: Vec<Buffered>,
    settled: bool,
}

impl std::fmt::Debug for ConfirmsTestTransaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConfirmsTestTransaction")
            .field("buffered", &self.buffered.len())
            .field("settled", &self.settled)
            .finish_non_exhaustive()
    }
}

impl Drop for ConfirmsTestTransaction {
    fn drop(&mut self) {
        // Same contract as the live transaction: a drop can only discard, and the warning marks
        // that as an abort the caller never wrote.
        if !self.settled {
            warn!(
                target: "ruststream_lapin",
                buffered = self.buffered.len(),
                "owned transaction dropped without commit or abort; its buffered messages are \
                 discarded"
            );
        }
    }
}

impl Transaction for ConfirmsTestTransaction {
    type Error = AmqpError;

    /// Buffers `msg` in this transaction; nothing reaches the router before
    /// [`commit`](Self::commit).
    ///
    /// # Errors
    ///
    /// Returns [`AmqpError::InvalidOptions`] when the routing key is empty, the one check the
    /// live transaction also makes before the broker would.
    fn publish(
        &mut self,
        msg: OutgoingMessage<'_>,
    ) -> impl Future<Output = Result<(), Self::Error>> {
        if msg.name().is_empty() {
            return ready(Err(AmqpError::InvalidOptions(
                "routing key must not be empty; on the default exchange it names the target queue"
                    .to_owned(),
            )));
        }
        self.buffered.push(Buffered::new(&msg));
        ready(Ok(()))
    }

    /// Routes the buffered messages in order.
    ///
    /// # Errors
    ///
    /// Returns [`AmqpError::Closed`] once the transport has shut down; the transaction is
    /// consumed either way.
    fn commit(mut self) -> impl Future<Output = Result<(), Self::Error>> {
        // Settled before the flush, like the live transaction: a failed commit has still
        // consumed the value.
        self.settled = true;
        let mut flushed = Ok(());
        for entry in &self.buffered {
            flushed = self.route.send_buffered(entry);
            if flushed.is_err() {
                break;
            }
        }
        ready(flushed)
    }

    /// Discards the buffered messages.
    ///
    /// # Errors
    ///
    /// Never fails.
    fn abort(mut self) -> impl Future<Output = Result<(), Self::Error>> {
        self.settled = true;
        ready(Ok(()))
    }
}

/// The in-process stand-in for [`ServerTxPublisher`](crate::ServerTxPublisher): the borrowed
/// transaction shape ([`TransactionalPublisher`]) and no owned one, exactly like its counterpart.
///
/// Paired from [`ServerTxPublish`]. What a handler observes is reproduced: publishes made between
/// [`begin_transaction`](TransactionalPublisher::begin_transaction) and
/// [`commit`](TransactionalPublisher::commit) become visible only at the commit, an
/// [`abort`](TransactionalPublisher::abort) makes them visible nowhere, and publishes outside a
/// transaction go straight out. Clones share the transaction state.
///
/// What is not reproduced is where the messages wait. A server transaction stages them on the
/// broker inside the channel: `tx.commit` makes the batch visible atomically, `tx.rollback`
/// discards it server-side, and a channel that dies mid-transaction rolls it back for you. Here
/// they are held in the client's own buffer, so a test proves the visibility boundary and nothing
/// about atomicity or about what a lost connection does to a transaction in flight. Both belong
/// to a live server, and the crate's integration tests exercise them there.
///
/// # Examples
///
/// ```
/// use ruststream::{Broker, OutgoingMessage, Publisher, TransactionalPublisher};
/// use ruststream_lapin::LapinPublish;
/// use ruststream_lapin::testing::LapinTestBroker;
///
/// # async fn demo() -> Result<(), ruststream_lapin::AmqpError> {
/// let broker = LapinTestBroker::new().connect().await?;
/// let publisher = broker.publisher(LapinPublish::default().server_tx());
///
/// publisher.begin_transaction().await?;
/// publisher
///     .publish(OutgoingMessage::new("ledger", b"{}".as_slice()))
///     .await?;
/// publisher.abort().await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct ServerTxTestPublisher {
    buffering: Buffering,
}

/// How the transaction diagnostics name this publisher.
const SERVER_TX: &str = "server-transactional test publisher";

impl Publisher for ServerTxTestPublisher {
    type Error = AmqpError;

    /// Routes `msg`, or stages it when a transaction is open on this handle.
    ///
    /// # Errors
    ///
    /// Returns [`AmqpError::InvalidOptions`] when the routing key is empty and
    /// [`AmqpError::Closed`] once the transport has shut down.
    fn publish(&self, msg: OutgoingMessage<'_>) -> impl Future<Output = Result<(), Self::Error>> {
        ready(self.buffering.publish(&msg))
    }
}

impl TransactionalPublisher for ServerTxTestPublisher {
    /// Opens the transaction.
    ///
    /// # Errors
    ///
    /// Returns [`AmqpError::Transaction`] when a transaction is already open; the open
    /// transaction is left untouched.
    fn begin_transaction(&self) -> impl Future<Output = Result<(), Self::Error>> {
        ready(self.buffering.begin(SERVER_TX))
    }

    /// Makes the staged messages visible, in publish order.
    ///
    /// # Errors
    ///
    /// Returns [`AmqpError::Transaction`] when no transaction is open and [`AmqpError::Closed`]
    /// once the transport has shut down.
    fn commit(&self) -> impl Future<Output = Result<(), Self::Error>> {
        ready(self.buffering.commit(SERVER_TX))
    }

    /// Discards the staged messages.
    ///
    /// # Errors
    ///
    /// Returns [`AmqpError::Transaction`] when no transaction is open.
    fn abort(&self) -> impl Future<Output = Result<(), Self::Error>> {
        ready(self.buffering.abort(SERVER_TX))
    }
}

impl PublishPolicy<ConnectedLapinTestBroker> for ServerTxPublish {
    type Live = ServerTxTestPublisher;

    fn pair(
        self,
        connected: &ConnectedLapinTestBroker,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(LapinTestPublishPolicy::bind(self, connected)))
    }
}

impl LapinTestPublishPolicy for ServerTxPublish {
    fn bind(self, connected: &ConnectedLapinTestBroker) -> Self::Live {
        ServerTxTestPublisher {
            buffering: Buffering::new(connected),
        }
    }
}
