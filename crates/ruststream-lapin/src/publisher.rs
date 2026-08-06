//! The live half of publishing: the publishers a policy pairs into.
//!
//! Each of them exists only from a [`ConnectedLapinBroker`], so it always has a connection; the
//! declaration half (what to publish and how) lives in [`crate::publish_policy`].

use std::sync::{Arc, Mutex};

use bytes::Bytes;
use lapin::options::{BasicPublishOptions, ConfirmSelectOptions};
use lapin::{BasicProperties, Channel};
use lapin::{Confirmation, PublisherConfirm};
use ruststream::{Headers, OutgoingMessage, Publisher, TransactionalPublisher};
use tokio::sync::OnceCell;

use crate::broker::{AmqpConnection, ConnectedLapinBroker};
use crate::convert;
use crate::error::AmqpError;
use crate::publish_policy::PublishOptions;

/// One buffered publish: routing key, payload, headers.
pub(crate) type Buffered = (String, Bytes, Headers);

pub(crate) async fn do_publish(
    channel: &Channel,
    exchange: &str,
    routing_key: &str,
    payload: &[u8],
    properties: BasicProperties,
) -> Result<PublisherConfirm, AmqpError> {
    channel
        .basic_publish(
            convert::short(exchange, "exchange name")?,
            convert::short(routing_key, "routing key")?,
            BasicPublishOptions::default(),
            payload,
            properties,
        )
        .await
        .map_err(AmqpError::publish)
}

/// The live fire-and-forget publisher, on the connection's shared publish channel. Cheap to
/// clone.
///
/// Paired from [`LapinPublish`](crate::LapinPublish), so it always has a connection. It aliases
/// that connection, though, and may outlive it: after the broker shuts down every publish
/// reports [`AmqpError::Closed`] instead of silently succeeding against a dead connection.
#[derive(Debug, Clone)]
pub struct LapinPublisher {
    conn: Arc<AmqpConnection>,
    options: PublishOptions,
}

impl LapinPublisher {
    pub(crate) fn new(connected: &ConnectedLapinBroker, options: PublishOptions) -> Self {
        Self {
            conn: Arc::clone(connected.connection()),
            options,
        }
    }
}

impl Publisher for LapinPublisher {
    type Error = AmqpError;

    /// Publishes `msg` without waiting for a broker confirm.
    ///
    /// # Errors
    ///
    /// Returns [`AmqpError::Closed`] once the broker has shut down and
    /// [`AmqpError::Publish`] when the channel rejects the frame.
    ///
    /// # Cancel safety
    ///
    /// Not cancel safe: dropping the future may leave the message published or not.
    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        let channel = self.conn.live_publish_channel(msg.name())?;
        let properties = convert::properties_for_publish(msg.headers(), self.options.persistent)?;
        // Without confirm_select on the channel the returned confirm resolves to NotRequested;
        // dropping it does not lose anything.
        let _confirm = do_publish(
            channel,
            &self.options.exchange,
            msg.name(),
            msg.payload(),
            properties,
        )
        .await?;
        Ok(())
    }
}

/// The live publisher that awaits broker confirms for every message.
///
/// Outside a transaction each [`publish`](Publisher::publish) resolves only once the broker
/// confirmed the message. Confirms buffer client-side, so this publisher offers both transaction
/// kinds:
///
/// * owned ([`OwnedTransactions`](ruststream::OwnedTransactions), the natural fit): every
///   [`transaction`](ruststream::OwnedTransactions::transaction) call opens an independent
///   [`ConfirmsTransaction`](crate::ConfirmsTransaction) that owns its buffer, so any number can
///   be open on one handle and the handle keeps publishing directly meanwhile;
/// * borrowed ([`TransactionalPublisher`]): the handle carries one buffer between
///   [`begin_transaction`](TransactionalPublisher::begin_transaction) and
///   [`commit`](TransactionalPublisher::commit), so a second begin while one is open errors.
///
/// Either way `commit` publishes the buffer in order and awaits all confirms, and `abort`
/// discards it without touching the broker.
///
/// Clones share one confirm channel and one handle-level transaction buffer. Like every live
/// publisher it aliases the connection and may outlive it: after shutdown every operation
/// reports [`AmqpError::Closed`].
#[derive(Debug, Clone)]
pub struct ConfirmsPublisher {
    conn: Arc<AmqpConnection>,
    options: PublishOptions,
    channel: Arc<OnceCell<Channel>>,
    txn: Arc<Mutex<Option<Vec<Buffered>>>>,
}

impl ConfirmsPublisher {
    pub(crate) fn new(connected: &ConnectedLapinBroker, options: PublishOptions) -> Self {
        Self {
            conn: Arc::clone(connected.connection()),
            options,
            channel: Arc::new(OnceCell::new()),
            txn: Arc::new(Mutex::new(None)),
        }
    }

    /// The confirm channel, opened on first use.
    ///
    /// Why lazily and not at pairing time: pairing is a synchronous constructor call (see
    /// [`LapinPublishPolicy`]), and a publisher that never publishes should hold no channel.
    async fn channel(&self, target: &str) -> Result<&Channel, AmqpError> {
        self.channel
            .get_or_try_init(|| async {
                let channel = self
                    .conn
                    .live_connection(target)?
                    .create_channel()
                    .await
                    .map_err(AmqpError::publish)?;
                channel
                    .confirm_select(ConfirmSelectOptions::default())
                    .await
                    .map_err(AmqpError::publish)?;
                Ok(channel)
            })
            .await
    }

    /// Publishes `buffered` in order on the confirm channel and awaits every confirm.
    ///
    /// The flush of the owned transaction kind ([`ConfirmsTransaction`]), whose contract loses
    /// the buffer on a failed commit - redelivery of the inputs is the recovery path - so it
    /// needs no bookkeeping about what was sent. The borrowed kind keeps its own flush: its
    /// buffer is shared with the handle, which is state this one does not have.
    pub(crate) async fn flush_owned(&self, buffered: &[Buffered]) -> Result<(), AmqpError> {
        // The whole buffer rides one channel; the first routing key names the flush in any
        // connection-level diagnostic.
        let Some((first_key, _, _)) = buffered.first() else {
            return Ok(());
        };
        self.conn.ensure_live(first_key)?;
        let channel = self.channel(first_key).await?;

        let mut confirms = Vec::with_capacity(buffered.len());
        for (routing_key, payload, headers) in buffered {
            let properties = convert::properties_for_publish(headers, self.options.persistent)?;
            let confirm = do_publish(
                channel,
                &self.options.exchange,
                routing_key,
                payload,
                properties,
            )
            .await?;
            confirms.push((routing_key, confirm));
        }
        for (routing_key, confirm) in confirms {
            let confirmation = confirm.await.map_err(AmqpError::publish)?;
            confirmation_ok(&confirmation, routing_key)?;
        }
        Ok(())
    }

    async fn publish_confirmed(
        &self,
        routing_key: &str,
        payload: &[u8],
        headers: &Headers,
    ) -> Result<(), AmqpError> {
        self.conn.ensure_live(routing_key)?;
        let channel = self.channel(routing_key).await?;
        let properties = convert::properties_for_publish(headers, self.options.persistent)?;
        let confirm = do_publish(
            channel,
            &self.options.exchange,
            routing_key,
            payload,
            properties,
        )
        .await?
        .await
        .map_err(AmqpError::publish)?;
        confirmation_ok(&confirm, routing_key)
    }
}

fn confirmation_ok(confirmation: &Confirmation, routing_key: &str) -> Result<(), AmqpError> {
    if confirmation.is_nack() {
        return Err(AmqpError::Publish(
            format!("the broker negatively confirmed the publish to {routing_key:?}").into(),
        ));
    }
    Ok(())
}

impl Publisher for ConfirmsPublisher {
    type Error = AmqpError;

    /// Publishes `msg`, awaiting the broker confirm (or buffering inside a transaction).
    ///
    /// # Errors
    ///
    /// Returns [`AmqpError::Closed`] once the broker has shut down and [`AmqpError::Publish`]
    /// when the channel rejects the frame or the broker returns a negative confirm.
    ///
    /// # Cancel safety
    ///
    /// Not cancel safe outside a transaction: dropping the future may leave the message
    /// published but unconfirmed. Inside a transaction buffering is synchronous and dropping the
    /// future is harmless.
    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
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
        self.publish_confirmed(msg.name(), msg.payload(), msg.headers())
            .await
    }
}

impl TransactionalPublisher for ConfirmsPublisher {
    /// Opens the buffering transaction.
    ///
    /// # Errors
    ///
    /// Returns [`AmqpError::Transaction`] when a transaction is already open on this handle;
    /// the open transaction is left untouched.
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
                "a transaction is already open on this confirms publisher; commit or abort it \
                 before beginning another"
                    .to_owned(),
            ));
        }
        Ok(())
    }

    /// Publishes the buffered messages in order and awaits every confirm.
    ///
    /// # Errors
    ///
    /// Returns [`AmqpError::Transaction`] when no transaction is open, and
    /// [`AmqpError::Publish`] when any message fails to publish or the broker returns a negative
    /// confirm. Messages already flushed stay published: publisher confirms give durability per
    /// message, not atomicity across them (use [`ServerTxPublish`](crate::ServerTxPublish) for
    /// that).
    async fn commit(&self) -> Result<(), Self::Error> {
        let buffered = {
            let mut txn = self.txn.lock().expect("transaction buffer mutex poisoned");
            txn.take()
        };
        let Some(buffered) = buffered else {
            return Err(AmqpError::Transaction(
                "commit with no open transaction on this confirms publisher".to_owned(),
            ));
        };
        if buffered.is_empty() {
            return Ok(());
        }

        let target = buffered[0].0.as_str();
        self.conn.ensure_live(target)?;
        let channel = self.channel(target).await?;
        let mut confirms = Vec::with_capacity(buffered.len());
        for (routing_key, payload, headers) in &buffered {
            let properties = convert::properties_for_publish(headers, self.options.persistent)?;
            let confirm = do_publish(
                channel,
                &self.options.exchange,
                routing_key,
                payload,
                properties,
            )
            .await?;
            confirms.push((routing_key, confirm));
        }
        for (routing_key, confirm) in confirms {
            let confirmation = confirm.await.map_err(AmqpError::publish)?;
            confirmation_ok(&confirmation, routing_key)?;
        }
        Ok(())
    }

    /// Discards the buffered messages without publishing anything.
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
                "abort with no open transaction on this confirms publisher".to_owned(),
            ));
        }
        Ok(())
    }
}

/// The live publisher backed by AMQP server transactions (`tx.select` / `tx.commit` /
/// `tx.rollback`).
///
/// Between [`begin_transaction`](TransactionalPublisher::begin_transaction) and
/// [`commit`](TransactionalPublisher::commit) messages accumulate on the broker inside the
/// channel transaction and become visible atomically at commit;
/// [`abort`](TransactionalPublisher::abort) rolls them back server-side. Outside a transaction
/// [`publish`](Publisher::publish) behaves like the fire-and-forget publisher.
///
/// Only the borrowed transaction kind ([`TransactionalPublisher`]) applies here, unlike
/// [`ConfirmsPublisher`]: `tx.select` puts the channel itself into transactional mode, so the
/// transaction is channel state with exactly one instance, and there is no buffer for an owned
/// [`Transaction`](ruststream::Transaction) value to own.
///
/// Clones share the transactional channel and its open/closed state. Interleaving `publish`
/// and `begin_transaction`/`commit` from concurrent tasks is not supported: which side of the
/// transaction boundary a concurrent publish lands on would be a race either way. Like every
/// live publisher it aliases the connection and may outlive it: after shutdown every operation
/// reports [`AmqpError::Closed`].
#[derive(Debug, Clone)]
pub struct ServerTxPublisher {
    conn: Arc<AmqpConnection>,
    options: PublishOptions,
    channel: Arc<OnceCell<Channel>>,
    open: Arc<Mutex<bool>>,
}

impl ServerTxPublisher {
    pub(crate) fn new(connected: &ConnectedLapinBroker, options: PublishOptions) -> Self {
        Self {
            conn: Arc::clone(connected.connection()),
            options,
            channel: Arc::new(OnceCell::new()),
            open: Arc::new(Mutex::new(false)),
        }
    }

    /// The transactional channel, opened on first use; see [`ConfirmsPublisher::channel`] for
    /// why it is not opened at pairing time.
    async fn tx_channel(&self, target: &str) -> Result<&Channel, AmqpError> {
        self.channel
            .get_or_try_init(|| async {
                let channel = self
                    .conn
                    .live_connection(target)?
                    .create_channel()
                    .await
                    .map_err(AmqpError::publish)?;
                channel.tx_select().await.map_err(AmqpError::publish)?;
                Ok(channel)
            })
            .await
    }

    fn is_open(&self) -> bool {
        *self.open.lock().expect("transaction state mutex poisoned")
    }

    fn set_open(&self, open: bool) {
        *self.open.lock().expect("transaction state mutex poisoned") = open;
    }
}

impl Publisher for ServerTxPublisher {
    type Error = AmqpError;

    /// Publishes `msg`: into the open server transaction, or plainly when none is open.
    ///
    /// # Errors
    ///
    /// Returns [`AmqpError::Closed`] once the broker has shut down and [`AmqpError::Publish`]
    /// when the channel rejects the frame.
    ///
    /// # Cancel safety
    ///
    /// Not cancel safe: dropping the future may leave the message queued in the transaction or
    /// not.
    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        let properties = convert::properties_for_publish(msg.headers(), self.options.persistent)?;
        let channel = if self.is_open() {
            self.conn.ensure_live(msg.name())?;
            self.tx_channel(msg.name()).await?
        } else {
            self.conn.live_publish_channel(msg.name())?
        };
        let _confirm = do_publish(
            channel,
            &self.options.exchange,
            msg.name(),
            msg.payload(),
            properties,
        )
        .await?;
        Ok(())
    }
}

/// The transaction target named in diagnostics: server transactions are a property of the
/// channel, not of one routing key.
const TX_TARGET: &str = "the transactional channel";

impl TransactionalPublisher for ServerTxPublisher {
    /// Opens a server transaction (`tx.select` on first use).
    ///
    /// # Errors
    ///
    /// Returns [`AmqpError::Transaction`] when a transaction is already open on this handle
    /// (the open transaction is left untouched), [`AmqpError::Closed`] once the broker has shut
    /// down, and [`AmqpError::Publish`] when the transactional channel cannot be set up.
    async fn begin_transaction(&self) -> Result<(), Self::Error> {
        if self.is_open() {
            return Err(AmqpError::Transaction(
                "a transaction is already open on this server-transactional publisher; commit or \
                 abort it before beginning another"
                    .to_owned(),
            ));
        }
        self.conn.ensure_live(TX_TARGET)?;
        self.tx_channel(TX_TARGET).await?;
        self.set_open(true);
        Ok(())
    }

    /// Commits the open server transaction.
    ///
    /// # Errors
    ///
    /// Returns [`AmqpError::Transaction`] when no transaction is open, and
    /// [`AmqpError::Publish`] when `tx.commit` fails; the transaction state on the broker is
    /// then unknown (the channel may be closed) and the publisher should be discarded.
    async fn commit(&self) -> Result<(), Self::Error> {
        if !self.is_open() {
            return Err(AmqpError::Transaction(
                "commit with no open transaction on this server-transactional publisher".to_owned(),
            ));
        }
        self.conn.ensure_live(TX_TARGET)?;
        let channel = self.tx_channel(TX_TARGET).await?;
        channel.tx_commit().await.map_err(AmqpError::publish)?;
        self.set_open(false);
        Ok(())
    }

    /// Rolls back the open server transaction.
    ///
    /// # Errors
    ///
    /// Returns [`AmqpError::Transaction`] when no transaction is open, and
    /// [`AmqpError::Publish`] when `tx.rollback` fails.
    async fn abort(&self) -> Result<(), Self::Error> {
        if !self.is_open() {
            return Err(AmqpError::Transaction(
                "abort with no open transaction on this server-transactional publisher".to_owned(),
            ));
        }
        self.conn.ensure_live(TX_TARGET)?;
        let channel = self.tx_channel(TX_TARGET).await?;
        channel.tx_rollback().await.map_err(AmqpError::publish)?;
        self.set_open(false);
        Ok(())
    }
}
