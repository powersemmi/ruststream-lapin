//! The in-process publish pair: the [`LapinTestPublish`] policy and its live
//! [`LapinTestPublisher`].

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

type Buffered = (String, Bytes, HeaderMap);

/// The in-process publish policy, mirroring [`LapinPublish`](crate::LapinPublish) on the real
/// broker.
///
/// The router matches queue names exactly, so exchange and persistence carry no meaning here and
/// the policy is a unit marker.
///
/// # Examples
///
/// ```
/// use ruststream_lapin::testing::LapinTestPublish;
///
/// let policy = LapinTestPublish;
/// # let _ = policy;
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[must_use]
pub struct LapinTestPublish;

impl LapinTestPublish {
    /// Pairs the policy with the connected test broker.
    #[must_use]
    pub fn bind(self, connected: &ConnectedLapinTestBroker) -> LapinTestPublisher {
        LapinTestPublisher {
            state: connected.state(),
            txn: Arc::new(Mutex::new(None)),
        }
    }
}

impl PublishPolicy<ConnectedLapinTestBroker> for LapinTestPublish {
    type Live = LapinTestPublisher;

    fn pair(
        self,
        connected: &ConnectedLapinTestBroker,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(self.bind(connected)))
    }
}

/// The live publisher into the in-process router.
///
/// Mirrors [`ConfirmsPublisher`](crate::ConfirmsPublisher) transaction semantics: publishes
/// buffer between `begin_transaction` and `commit`, `abort` discards them, and a call with no
/// open transaction errors. Clones share the transaction buffer. Like the real publishers it
/// aliases the transport and may outlive it: after the broker shuts down every publish reports
/// [`AmqpError::Closed`].
#[derive(Debug, Clone)]
pub struct LapinTestPublisher {
    state: Arc<TestBrokerState>,
    txn: Arc<Mutex<Option<Vec<Buffered>>>>,
}

impl LapinTestPublisher {
    fn route(&self, queue: &str, payload: &Bytes, headers: &HeaderMap) {
        self.state
            .router
            .publish(queue, payload, headers, self.state.coordinator().as_ref());
    }
}

impl Publisher for LapinTestPublisher {
    type Error = AmqpError;

    /// Routes `msg` to subscribers of the queue named by `msg.name()`.
    ///
    /// # Errors
    ///
    /// Returns [`AmqpError::InvalidOptions`] when the routing key is empty and
    /// [`AmqpError::Closed`] once the transport has shut down.
    fn publish(&self, msg: OutgoingMessage<'_>) -> impl Future<Output = Result<(), Self::Error>> {
        if msg.name().is_empty() {
            return ready(Err(AmqpError::InvalidOptions(
                "routing key must not be empty; on the default exchange it names the target queue"
                    .to_owned(),
            )));
        }
        if let Err(err) = self.state.ensure_live(msg.name()) {
            return ready(Err(err));
        }
        {
            let mut txn = self.txn.lock().expect("transaction buffer mutex poisoned");
            if let Some(buffer) = txn.as_mut() {
                buffer.push((
                    msg.name().to_owned(),
                    Bytes::copy_from_slice(msg.payload()),
                    msg.headers().clone(),
                ));
                return ready(Ok(()));
            }
        }
        self.route(
            msg.name(),
            &Bytes::copy_from_slice(msg.payload()),
            msg.headers(),
        );
        ready(Ok(()))
    }
}

impl TransactionalPublisher for LapinTestPublisher {
    /// Opens the buffering transaction.
    ///
    /// # Errors
    ///
    /// Returns [`AmqpError::Transaction`] when a transaction is already open; the open
    /// transaction is left untouched.
    fn begin_transaction(&self) -> impl Future<Output = Result<(), Self::Error>> {
        let already_open = {
            let mut txn = self.txn.lock().expect("transaction buffer mutex poisoned");
            let open = txn.is_some();
            if !open {
                *txn = Some(Vec::new());
            }
            open
        };
        if already_open {
            return ready(Err(AmqpError::Transaction(
                "a transaction is already open on this test publisher; commit or abort it before \
                 beginning another"
                    .to_owned(),
            )));
        }
        ready(Ok(()))
    }

    /// Replays the buffered publishes in order.
    ///
    /// # Errors
    ///
    /// Returns [`AmqpError::Transaction`] when no transaction is open and [`AmqpError::Closed`]
    /// once the transport has shut down.
    fn commit(&self) -> impl Future<Output = Result<(), Self::Error>> {
        let buffered = {
            let mut txn = self.txn.lock().expect("transaction buffer mutex poisoned");
            txn.take()
        };
        let Some(buffered) = buffered else {
            return ready(Err(AmqpError::Transaction(
                "commit with no open transaction on this test publisher".to_owned(),
            )));
        };
        for (queue, payload, headers) in buffered {
            if let Err(err) = self.state.ensure_live(&queue) {
                return ready(Err(err));
            }
            self.route(&queue, &payload, &headers);
        }
        ready(Ok(()))
    }

    /// Discards the buffered publishes.
    ///
    /// # Errors
    ///
    /// Returns [`AmqpError::Transaction`] when no transaction is open.
    fn abort(&self) -> impl Future<Output = Result<(), Self::Error>> {
        let discarded = self
            .txn
            .lock()
            .expect("transaction buffer mutex poisoned")
            .take();
        if discarded.is_none() {
            return ready(Err(AmqpError::Transaction(
                "abort with no open transaction on this test publisher".to_owned(),
            )));
        }
        ready(Ok(()))
    }
}

/// Owned transactions, mirroring [`ConfirmsPublisher`](crate::ConfirmsPublisher): every call
/// opens an independent buffer-owning [`LapinTestTransaction`], so any number can be open at
/// once and the publisher keeps routing directly meanwhile.
impl OwnedTransactions for LapinTestPublisher {
    type Transaction = LapinTestTransaction;

    /// Opens a transaction owned by the returned value.
    ///
    /// # Errors
    ///
    /// Never fails: opening allocates a buffer and never touches the router.
    fn transaction(&self) -> impl Future<Output = Result<Self::Transaction, Self::Error>> {
        ready(Ok(LapinTestTransaction {
            publisher: self.clone(),
            buffered: Vec::new(),
            settled: false,
        }))
    }
}

/// An owned in-process transaction, opened by
/// [`transaction`](OwnedTransactions::transaction) on a [`LapinTestPublisher`].
///
/// A private buffer routed in publish order on commit and discarded on abort, mirroring
/// [`ConfirmsTransaction`](crate::ConfirmsTransaction).
///
/// # Examples
///
/// ```
/// use ruststream::{Broker, OutgoingMessage, OwnedTransactions, Transaction};
/// use ruststream_lapin::testing::{LapinTestBroker, LapinTestPublish};
///
/// # async fn demo() -> Result<(), ruststream_lapin::AmqpError> {
/// let broker = LapinTestBroker::new().connect().await?;
/// let mut txn = broker.publisher(LapinTestPublish).transaction().await?;
/// txn.publish(OutgoingMessage::new("orders", b"{}".as_slice())).await?;
/// txn.commit().await?;
/// # Ok(())
/// # }
/// ```
#[must_use = "a transaction does nothing until settled with commit() or abort()"]
pub struct LapinTestTransaction {
    publisher: LapinTestPublisher,
    buffered: Vec<Buffered>,
    settled: bool,
}

impl std::fmt::Debug for LapinTestTransaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LapinTestTransaction")
            .field("buffered", &self.buffered.len())
            .field("settled", &self.settled)
            .finish_non_exhaustive()
    }
}

impl Drop for LapinTestTransaction {
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

impl Transaction for LapinTestTransaction {
    type Error = AmqpError;

    /// Buffers `msg` in this transaction; nothing reaches the router before
    /// [`commit`](Self::commit).
    ///
    /// # Errors
    ///
    /// Returns [`AmqpError::InvalidOptions`] when the routing key is empty, the one check the
    /// live publisher also makes before the broker would.
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
        self.buffered.push((
            msg.name().to_owned(),
            Bytes::copy_from_slice(msg.payload()),
            msg.headers().clone(),
        ));
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
        for (queue, payload, headers) in &self.buffered {
            if let Err(err) = self.publisher.state.ensure_live(queue) {
                return ready(Err(err));
            }
            self.publisher.route(queue, payload, headers);
        }
        ready(Ok(()))
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
