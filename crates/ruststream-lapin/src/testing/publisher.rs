//! The in-process publisher, with buffered transactions.

use std::sync::{Arc, Mutex};

use bytes::Bytes;
use ruststream::{Headers, OutgoingMessage, Publisher, TransactionalPublisher};

use super::broker::TestBrokerState;
use crate::error::AmqpError;

type Buffered = (String, Bytes, Headers);

/// Publisher into the in-process router.
///
/// Mirrors [`ConfirmsPublisher`](crate::ConfirmsPublisher) transaction semantics: publishes
/// buffer between `begin_transaction` and `commit`, and `abort` discards them. Clones share the
/// transaction buffer.
#[derive(Debug, Clone)]
pub struct LapinTestPublisher {
    state: Arc<TestBrokerState>,
    txn: Arc<Mutex<Option<Vec<Buffered>>>>,
}

impl LapinTestPublisher {
    pub(crate) fn new(state: Arc<TestBrokerState>) -> Self {
        Self {
            state,
            txn: Arc::new(Mutex::new(None)),
        }
    }

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
    /// Returns [`AmqpError::InvalidOptions`] when the routing key is empty.
    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        if msg.name().is_empty() {
            return Err(AmqpError::InvalidOptions(
                "routing key must not be empty; on the default exchange it names the target queue"
                    .to_owned(),
            ));
        }
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
    /// Opens the buffering transaction; a no-op when one is already open.
    ///
    /// # Errors
    ///
    /// Never fails.
    async fn begin_transaction(&self) -> Result<(), Self::Error> {
        self.txn
            .lock()
            .expect("transaction buffer mutex poisoned")
            .get_or_insert_with(Vec::new);
        Ok(())
    }

    /// Replays the buffered publishes in order; a no-op when no transaction is open.
    ///
    /// # Errors
    ///
    /// Never fails.
    async fn commit(&self) -> Result<(), Self::Error> {
        let buffered = {
            let mut txn = self.txn.lock().expect("transaction buffer mutex poisoned");
            txn.take()
        };
        if let Some(buffered) = buffered {
            for (queue, payload, headers) in buffered {
                self.route(&queue, &payload, &headers);
            }
        }
        Ok(())
    }

    /// Discards the buffered publishes; a no-op when no transaction is open.
    ///
    /// # Errors
    ///
    /// Never fails.
    async fn abort(&self) -> Result<(), Self::Error> {
        self.txn
            .lock()
            .expect("transaction buffer mutex poisoned")
            .take();
        Ok(())
    }
}
