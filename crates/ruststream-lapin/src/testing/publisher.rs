//! The in-process publish pair: the [`LapinTestPublish`] policy and its live
//! [`LapinTestPublisher`].

use std::sync::{Arc, Mutex};

use bytes::Bytes;
use ruststream::{
    Headers, OutgoingMessage, PairError, PublishPolicy, Publisher, TransactionalPublisher,
};

use super::broker::{ConnectedLapinTestBroker, TestBrokerState};
use crate::error::AmqpError;

type Buffered = (String, Bytes, Headers);

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

    async fn pair(self, connected: &ConnectedLapinTestBroker) -> Result<Self::Live, PairError> {
        Ok(self.bind(connected))
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
    fn route(&self, queue: &str, payload: &Bytes, headers: &Headers) {
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
    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        if msg.name().is_empty() {
            return Err(AmqpError::InvalidOptions(
                "routing key must not be empty; on the default exchange it names the target queue"
                    .to_owned(),
            ));
        }
        self.state.ensure_live(msg.name())?;
        {
            let mut txn = self.txn.lock().expect("transaction buffer mutex poisoned");
            if let Some(buffer) = txn.as_mut() {
                buffer.push((
                    msg.name().to_owned(),
                    Bytes::copy_from_slice(msg.payload()),
                    msg.headers().clone(),
                ));
                return Ok(());
            }
        }
        self.route(
            msg.name(),
            &Bytes::copy_from_slice(msg.payload()),
            msg.headers(),
        );
        Ok(())
    }
}

impl TransactionalPublisher for LapinTestPublisher {
    /// Opens the buffering transaction.
    ///
    /// # Errors
    ///
    /// Returns [`AmqpError::Transaction`] when a transaction is already open; the open
    /// transaction is left untouched.
    async fn begin_transaction(&self) -> Result<(), Self::Error> {
        let already_open = {
            let mut txn = self.txn.lock().expect("transaction buffer mutex poisoned");
            let open = txn.is_some();
            if !open {
                *txn = Some(Vec::new());
            }
            open
        };
        if already_open {
            return Err(AmqpError::Transaction(
                "a transaction is already open on this test publisher; commit or abort it before \
                 beginning another"
                    .to_owned(),
            ));
        }
        Ok(())
    }

    /// Replays the buffered publishes in order.
    ///
    /// # Errors
    ///
    /// Returns [`AmqpError::Transaction`] when no transaction is open and [`AmqpError::Closed`]
    /// once the transport has shut down.
    async fn commit(&self) -> Result<(), Self::Error> {
        let buffered = {
            let mut txn = self.txn.lock().expect("transaction buffer mutex poisoned");
            txn.take()
        };
        let Some(buffered) = buffered else {
            return Err(AmqpError::Transaction(
                "commit with no open transaction on this test publisher".to_owned(),
            ));
        };
        for (queue, payload, headers) in buffered {
            self.state.ensure_live(&queue)?;
            self.route(&queue, &payload, &headers);
        }
        Ok(())
    }

    /// Discards the buffered publishes.
    ///
    /// # Errors
    ///
    /// Returns [`AmqpError::Transaction`] when no transaction is open.
    async fn abort(&self) -> Result<(), Self::Error> {
        let discarded = self
            .txn
            .lock()
            .expect("transaction buffer mutex poisoned")
            .take();
        if discarded.is_none() {
            return Err(AmqpError::Transaction(
                "abort with no open transaction on this test publisher".to_owned(),
            ));
        }
        Ok(())
    }
}
