//! The owned transaction kind for the confirms publisher.
//!
//! AMQP publisher confirms buffer client-side, so a transaction is a private buffer flushed at
//! commit: nothing is shared with the handle, and any number of them can be open on one
//! publisher at once. That is the owned kind ([`OwnedTransactions`]), the counterpart of the
//! borrowed [`TransactionalPublisher`](ruststream::TransactionalPublisher) the publisher also
//! implements.

use bytes::Bytes;
use ruststream::{OutgoingMessage, OwnedTransactions, Transaction};
use tracing::warn;

use crate::error::AmqpError;
use crate::publisher::{Buffered, ConfirmsPublisher};

/// An owned confirm-transaction, opened by
/// [`transaction`](OwnedTransactions::transaction) on a [`ConfirmsPublisher`].
///
/// A private publish buffer: [`publish`](Transaction::publish) appends to this value rather than
/// to the publisher, [`commit`](Transaction::commit) flushes the whole buffer on the confirm
/// channel and awaits every acknowledgement, and [`abort`](Transaction::abort) discards it
/// without touching the broker. Both settle by consuming `self`, so a double commit or a publish
/// after settling is a compile error.
///
/// Unlike the handle-level buffer of the borrowed kind, any number of these can be open on one
/// publisher at a time, and the publisher keeps publishing directly while they are.
///
/// # Examples
///
/// ```no_run
/// use ruststream::{Broker, OutgoingMessage, OwnedTransactions, Transaction};
/// use ruststream_lapin::{LapinBroker, LapinPublish};
///
/// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
/// let connected = LapinBroker::new("amqp://localhost:5672").connect().await?;
/// let publisher = connected.publisher(LapinPublish::default().confirms());
///
/// let mut orders = publisher.transaction().await?;
/// let mut audit = publisher.transaction().await?; // concurrent with `orders`
/// orders.publish(OutgoingMessage::new("orders", b"{}".as_slice())).await?;
/// audit.publish(OutgoingMessage::new("audit", b"{}".as_slice())).await?;
/// orders.commit().await?;
/// audit.commit().await?;
/// # Ok(())
/// # }
/// ```
#[must_use = "a transaction does nothing until settled with commit() or abort()"]
pub struct ConfirmsTransaction {
    publisher: ConfirmsPublisher,
    buffered: Vec<Buffered>,
    settled: bool,
}

impl std::fmt::Debug for ConfirmsTransaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConfirmsTransaction")
            .field("buffered", &self.buffered.len())
            .field("settled", &self.settled)
            .finish_non_exhaustive()
    }
}

impl Drop for ConfirmsTransaction {
    fn drop(&mut self) {
        // Destructors cannot run async work, so a drop can only discard the buffer; the warning
        // marks that as an abort the caller never wrote, which is almost always a missing commit.
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

impl Transaction for ConfirmsTransaction {
    type Error = AmqpError;

    /// Buffers `msg` in this transaction; nothing reaches the broker before
    /// [`commit`](Self::commit).
    ///
    /// # Errors
    ///
    /// Infallible in practice: buffering is local to this value, and a closed connection or a
    /// rejected frame surfaces at the commit, which is the visibility point.
    async fn publish(&mut self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        self.buffered.push((
            msg.name().to_owned(),
            Bytes::copy_from_slice(msg.payload()),
            msg.headers().clone(),
        ));
        Ok(())
    }

    /// Publishes the buffered messages in order and awaits every confirm.
    ///
    /// # Errors
    ///
    /// Returns [`AmqpError::Closed`] once the broker has shut down, and [`AmqpError::Publish`]
    /// when a message fails to publish or the broker returns a negative confirm. A failed commit
    /// has still consumed the transaction and its buffer is lost: redelivery of the inputs, not
    /// resubmission of the buffer, is the recovery path. Messages already flushed stay
    /// published - publisher confirms give durability per message, not atomicity across them
    /// (use [`ServerTxPublish`](crate::ServerTxPublish) for that).
    ///
    /// # Cancel safety
    ///
    /// Not cancel safe: dropping the future mid-flush leaves an unknown prefix of the buffer
    /// published.
    async fn commit(mut self) -> Result<(), Self::Error> {
        // Settled before the flush: a failed commit has still consumed the transaction, so the
        // drop warning must not fire on the way out.
        self.settled = true;
        self.publisher.flush_owned(&self.buffered).await
    }

    /// Discards the buffered messages without touching the broker.
    ///
    /// # Errors
    ///
    /// Never fails: nothing was staged on the broker.
    async fn abort(mut self) -> Result<(), Self::Error> {
        self.settled = true;
        Ok(())
    }
}

/// Owned transactions: every [`transaction`](OwnedTransactions::transaction) call opens an
/// independent buffer-owning [`ConfirmsTransaction`], so any number can be open concurrently on
/// one handle, next to (and unaffected by) the handle-level borrowed transaction.
impl OwnedTransactions for ConfirmsPublisher {
    type Transaction = ConfirmsTransaction;

    /// Opens a transaction owned by the returned value.
    ///
    /// # Errors
    ///
    /// Infallible in practice: opening allocates a buffer and never touches the broker, exactly
    /// like the borrowed `begin_transaction`.
    async fn transaction(&self) -> Result<Self::Transaction, Self::Error> {
        Ok(ConfirmsTransaction {
            publisher: self.clone(),
            buffered: Vec::new(),
            settled: false,
        })
    }
}
