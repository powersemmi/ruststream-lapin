//! The publishers: fire-and-forget, confirm-transactional, and server-transactional.

use std::sync::{Arc, Mutex};

use bytes::Bytes;
use lapin::options::{BasicPublishOptions, ConfirmSelectOptions};
use lapin::{BasicProperties, Channel};
use lapin::{Confirmation, PublisherConfirm};
use ruststream::{Headers, OutgoingMessage, Publisher, TransactionalPublisher};
use tokio::sync::OnceCell;

use crate::broker::SharedConn;
use crate::convert;
use crate::error::AmqpError;

/// One buffered publish: routing key, payload, headers.
type Buffered = (String, Bytes, Headers);

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

/// Fire-and-forget publisher on the broker's shared publish channel.
///
/// [`OutgoingMessage::name`] is the routing key; the target exchange is a property of the
/// publisher (the default exchange unless [`exchange`](Self::exchange) says otherwise). On the
/// default exchange the routing key addresses the queue with that name.
///
/// Messages are published persistent (delivery mode 2) unless
/// [`persistent(false)`](Self::persistent) opts out.
///
/// Obtained from [`LapinBroker::publisher`](crate::LapinBroker::publisher); usable before
/// `Broker::connect` resolves the connection (publishing earlier returns
/// [`AmqpError::NotConnected`]).
#[derive(Debug, Clone)]
pub struct LapinPublisher {
    conn: SharedConn,
    exchange: String,
    persistent: bool,
}

impl LapinPublisher {
    pub(crate) fn new(conn: SharedConn) -> Self {
        Self {
            conn,
            exchange: String::new(),
            persistent: true,
        }
    }

    /// Publishes to `exchange` instead of the default exchange.
    #[must_use]
    pub fn exchange(mut self, exchange: impl Into<String>) -> Self {
        self.exchange = exchange.into();
        self
    }

    /// Whether messages are marked persistent (delivery mode 2). Defaults to `true`.
    #[must_use]
    pub fn persistent(mut self, persistent: bool) -> Self {
        self.persistent = persistent;
        self
    }

    /// Upgrades to a publisher that awaits broker confirms, with buffering transactions.
    ///
    /// The recommended transactional publisher: durable and much faster than AMQP server
    /// transactions.
    #[must_use]
    pub fn confirms(self) -> ConfirmsPublisher {
        ConfirmsPublisher {
            conn: self.conn,
            exchange: self.exchange,
            persistent: self.persistent,
            channel: Arc::new(OnceCell::new()),
            txn: Arc::new(Mutex::new(None)),
        }
    }

    /// Upgrades to a publisher backed by AMQP server transactions (`tx.select`).
    ///
    /// Server-side atomicity, at the cost of a synchronous commit round trip that is
    /// significantly slower than [`confirms`](Self::confirms).
    #[must_use]
    pub fn server_tx(self) -> ServerTxPublisher {
        ServerTxPublisher {
            conn: self.conn,
            exchange: self.exchange,
            persistent: self.persistent,
            channel: Arc::new(OnceCell::new()),
            open: Arc::new(Mutex::new(false)),
        }
    }
}

impl Publisher for LapinPublisher {
    type Error = AmqpError;

    /// Publishes `msg` without waiting for a broker confirm.
    ///
    /// # Errors
    ///
    /// Returns [`AmqpError::NotConnected`] before `Broker::connect` resolves the connection and
    /// [`AmqpError::Publish`] when the channel rejects the frame.
    ///
    /// # Cancel safety
    ///
    /// Not cancel safe: dropping the future may leave the message published or not.
    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        let state = self.conn.get().ok_or(AmqpError::NotConnected)?;
        let properties = convert::properties_for_publish(msg.headers(), self.persistent)?;
        // Without confirm_select on the channel the returned confirm resolves to NotRequested;
        // dropping it does not lose anything.
        let _confirm = do_publish(
            state.publish_channel(),
            &self.exchange,
            msg.name(),
            msg.payload(),
            properties,
        )
        .await?;
        Ok(())
    }
}

/// A publisher that awaits broker confirms for every message.
///
/// Outside a transaction each [`publish`](Publisher::publish) resolves only once the broker
/// confirmed the message. Between
/// [`begin_transaction`](TransactionalPublisher::begin_transaction) and
/// [`commit`](TransactionalPublisher::commit) messages buffer in memory; `commit` publishes them
/// in order and awaits all confirms, and [`abort`](TransactionalPublisher::abort) discards the
/// buffer without touching the broker.
///
/// Clones share one confirm channel and one transaction buffer.
#[derive(Debug, Clone)]
pub struct ConfirmsPublisher {
    conn: SharedConn,
    exchange: String,
    persistent: bool,
    channel: Arc<OnceCell<Channel>>,
    txn: Arc<Mutex<Option<Vec<Buffered>>>>,
}

impl ConfirmsPublisher {
    async fn channel(&self) -> Result<&Channel, AmqpError> {
        self.channel
            .get_or_try_init(|| async {
                let state = self.conn.get().ok_or(AmqpError::NotConnected)?;
                let channel = state
                    .connection()
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

    async fn publish_confirmed(
        &self,
        routing_key: &str,
        payload: &[u8],
        headers: &Headers,
    ) -> Result<(), AmqpError> {
        let channel = self.channel().await?;
        let properties = convert::properties_for_publish(headers, self.persistent)?;
        let confirm = do_publish(channel, &self.exchange, routing_key, payload, properties)
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
    /// Returns [`AmqpError::NotConnected`] before `Broker::connect` resolves the connection and
    /// [`AmqpError::Publish`] when the channel rejects the frame or the broker returns a
    /// negative confirm.
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
    /// Opens the buffering transaction; a no-op when one is already open.
    ///
    /// # Errors
    ///
    /// Never fails today; the signature leaves room for transport errors.
    async fn begin_transaction(&self) -> Result<(), Self::Error> {
        self.txn
            .lock()
            .expect("transaction buffer mutex poisoned")
            .get_or_insert_with(Vec::new);
        Ok(())
    }

    /// Publishes the buffered messages in order and awaits every confirm.
    ///
    /// # Errors
    ///
    /// Returns [`AmqpError::Publish`] when any message fails to publish or the broker returns a
    /// negative confirm. Messages already flushed stay published: publisher confirms give
    /// durability per message, not atomicity across them (use
    /// [`server_tx`](LapinPublisher::server_tx) for that).
    async fn commit(&self) -> Result<(), Self::Error> {
        let buffered = {
            let mut txn = self.txn.lock().expect("transaction buffer mutex poisoned");
            txn.take()
        };
        let Some(buffered) = buffered else {
            return Ok(());
        };
        if buffered.is_empty() {
            return Ok(());
        }

        let channel = self.channel().await?;
        let mut confirms = Vec::with_capacity(buffered.len());
        for (routing_key, payload, headers) in &buffered {
            let properties = convert::properties_for_publish(headers, self.persistent)?;
            let confirm =
                do_publish(channel, &self.exchange, routing_key, payload, properties).await?;
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
    /// Never fails today; the signature leaves room for transport errors.
    async fn abort(&self) -> Result<(), Self::Error> {
        self.txn
            .lock()
            .expect("transaction buffer mutex poisoned")
            .take();
        Ok(())
    }
}

/// A publisher backed by AMQP server transactions (`tx.select` / `tx.commit` / `tx.rollback`).
///
/// Between [`begin_transaction`](TransactionalPublisher::begin_transaction) and
/// [`commit`](TransactionalPublisher::commit) messages accumulate on the broker inside the
/// channel transaction and become visible atomically at commit;
/// [`abort`](TransactionalPublisher::abort) rolls them back server-side. Outside a transaction
/// [`publish`](Publisher::publish) behaves like the fire-and-forget publisher.
///
/// Clones share the transactional channel and its open/closed state. Interleaving `publish`
/// and `begin_transaction`/`commit` from concurrent tasks is not supported: which side of the
/// transaction boundary a concurrent publish lands on would be a race either way.
#[derive(Debug, Clone)]
pub struct ServerTxPublisher {
    conn: SharedConn,
    exchange: String,
    persistent: bool,
    channel: Arc<OnceCell<Channel>>,
    open: Arc<Mutex<bool>>,
}

impl ServerTxPublisher {
    async fn tx_channel(&self) -> Result<&Channel, AmqpError> {
        self.channel
            .get_or_try_init(|| async {
                let state = self.conn.get().ok_or(AmqpError::NotConnected)?;
                let channel = state
                    .connection()
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
    /// Returns [`AmqpError::NotConnected`] before `Broker::connect` resolves the connection and
    /// [`AmqpError::Publish`] when the channel rejects the frame.
    ///
    /// # Cancel safety
    ///
    /// Not cancel safe: dropping the future may leave the message queued in the transaction or
    /// not.
    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        let properties = convert::properties_for_publish(msg.headers(), self.persistent)?;
        let channel = if self.is_open() {
            self.tx_channel().await?
        } else {
            let state = self.conn.get().ok_or(AmqpError::NotConnected)?;
            state.publish_channel()
        };
        let _confirm = do_publish(
            channel,
            &self.exchange,
            msg.name(),
            msg.payload(),
            properties,
        )
        .await?;
        Ok(())
    }
}

impl TransactionalPublisher for ServerTxPublisher {
    /// Opens a server transaction (`tx.select` on first use); a no-op when one is open.
    ///
    /// # Errors
    ///
    /// Returns [`AmqpError::NotConnected`] before `Broker::connect` resolves the connection and
    /// [`AmqpError::Publish`] when the transactional channel cannot be set up.
    async fn begin_transaction(&self) -> Result<(), Self::Error> {
        self.tx_channel().await?;
        self.set_open(true);
        Ok(())
    }

    /// Commits the open server transaction; a no-op when none is open.
    ///
    /// # Errors
    ///
    /// Returns [`AmqpError::Publish`] when `tx.commit` fails; the transaction state on the
    /// broker is then unknown (the channel may be closed) and the publisher should be discarded.
    async fn commit(&self) -> Result<(), Self::Error> {
        if !self.is_open() {
            return Ok(());
        }
        let channel = self.tx_channel().await?;
        channel.tx_commit().await.map_err(AmqpError::publish)?;
        self.set_open(false);
        Ok(())
    }

    /// Rolls back the open server transaction; a no-op when none is open.
    ///
    /// # Errors
    ///
    /// Returns [`AmqpError::Publish`] when `tx.rollback` fails.
    async fn abort(&self) -> Result<(), Self::Error> {
        if !self.is_open() {
            return Ok(());
        }
        let channel = self.tx_channel().await?;
        channel.tx_rollback().await.map_err(AmqpError::publish)?;
        self.set_open(false);
        Ok(())
    }
}
