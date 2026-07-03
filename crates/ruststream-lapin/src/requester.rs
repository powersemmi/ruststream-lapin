//! Request/reply over `RabbitMQ` direct reply-to (`amq.rabbitmq.reply-to`).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use futures::StreamExt;
use lapin::Channel;
use lapin::options::{BasicConsumeOptions, BasicPublishOptions};
use lapin::types::{FieldTable, ShortString};
use ruststream::{OutgoingMessage, Publisher, RequestReply};
use tokio::sync::{OnceCell, oneshot};

use crate::broker::SharedConn;
use crate::convert;
use crate::error::AmqpError;
use crate::message::LapinMessage;

/// The pseudo-queue `RabbitMQ` rewrites per-request for direct reply-to.
const REPLY_TO: &str = "amq.rabbitmq.reply-to";

type Pending = Mutex<HashMap<String, oneshot::Sender<LapinMessage>>>;

/// A request/reply client over `RabbitMQ` direct reply-to.
///
/// [`request`](RequestReply::request) publishes to the routing key named by
/// [`OutgoingMessage::name`] (on the default exchange unless [`exchange`](Self::exchange) says
/// otherwise) with `reply-to` set to the direct reply-to pseudo-queue and a generated
/// `correlation-id`; the responder replies by publishing to the `reply-to` it received, echoing
/// the `correlation-id`.
///
/// Direct reply-to is at-most-once: replies live in channel state on one broker node, so a
/// dropped requester channel loses in-flight replies. The per-request timeout is the recovery
/// mechanism.
///
/// Requests are published transient (delivery mode 1) by default: a request nobody is waiting
/// for after the timeout gains nothing from surviving a broker restart. Opt into persistence
/// with [`persistent(true)`](Self::persistent).
///
/// Obtained from [`LapinBroker::requester`](crate::LapinBroker::requester). Clones share the
/// reply consumer and the pending-request table.
#[derive(Debug, Clone)]
pub struct LapinRequester {
    conn: SharedConn,
    exchange: String,
    persistent: bool,
    state: Arc<OnceCell<ReqState>>,
    pending: Arc<Pending>,
    next_id: Arc<AtomicU64>,
}

#[derive(Debug)]
struct ReqState {
    channel: Channel,
}

impl LapinRequester {
    pub(crate) fn new(conn: SharedConn) -> Self {
        Self {
            conn,
            exchange: String::new(),
            persistent: false,
            state: Arc::new(OnceCell::new()),
            pending: Arc::new(Mutex::new(HashMap::new())),
            next_id: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Publishes requests to `exchange` instead of the default exchange.
    #[must_use]
    pub fn exchange(mut self, exchange: impl Into<String>) -> Self {
        self.exchange = exchange.into();
        self
    }

    /// Whether requests are marked persistent (delivery mode 2). Defaults to `false`.
    #[must_use]
    pub fn persistent(mut self, persistent: bool) -> Self {
        self.persistent = persistent;
        self
    }

    /// Opens the requester channel and starts the reply consumer, once.
    ///
    /// The consumer MUST be up before the first publish carrying the direct reply-to address;
    /// `RabbitMQ` rejects such a publish with `PRECONDITION_FAILED` otherwise.
    async fn state(&self) -> Result<&ReqState, AmqpError> {
        self.state
            .get_or_try_init(|| async {
                let state = self.conn.get().ok_or(AmqpError::NotConnected)?;
                let channel = state
                    .connection()
                    .create_channel()
                    .await
                    .map_err(AmqpError::request)?;
                let consumer = channel
                    .basic_consume(
                        ShortString::from(REPLY_TO),
                        ShortString::default(),
                        BasicConsumeOptions {
                            no_ack: true,
                            ..BasicConsumeOptions::default()
                        },
                        FieldTable::default(),
                    )
                    .await
                    .map_err(AmqpError::request)?;

                // The task exits when the channel closes (consumer stream ends) or when every
                // requester clone is gone (Weak upgrade fails on the next reply).
                let pending = Arc::downgrade(&self.pending);
                tokio::spawn(dispatch_replies(consumer, pending));

                Ok(ReqState { channel })
            })
            .await
    }
}

async fn dispatch_replies(mut consumer: lapin::Consumer, pending: Weak<Pending>) {
    while let Some(delivery) = consumer.next().await {
        let Ok(delivery) = delivery else {
            // The channel is failing; consuming further would spin. Outstanding requests fail
            // by timeout.
            return;
        };
        let Some(pending) = pending.upgrade() else {
            return;
        };
        let correlation_id = delivery
            .properties
            .correlation_id()
            .as_ref()
            .map(ShortString::as_str);
        let Some(correlation_id) = correlation_id else {
            tracing::debug!("dropping direct reply-to delivery without a correlation-id");
            continue;
        };
        let waiter = pending
            .lock()
            .expect("pending requests mutex poisoned")
            .remove(correlation_id);
        match waiter {
            // The receiver may have timed out concurrently; nothing to do then.
            Some(tx) => drop(tx.send(LapinMessage::from_delivery_no_ack(delivery))),
            None => {
                tracing::debug!(
                    correlation_id,
                    "dropping direct reply-to delivery with no waiter"
                );
            }
        }
    }
}

impl Publisher for LapinRequester {
    type Error = AmqpError;

    /// Publishes `msg` on the requester channel without expecting a reply.
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
        let state = self.state().await?;
        let properties = convert::properties_for_publish(msg.headers(), self.persistent)?;
        let _confirm = state
            .channel
            .basic_publish(
                convert::short(&self.exchange, "exchange name")?,
                convert::short(msg.name(), "routing key")?,
                BasicPublishOptions::default(),
                msg.payload(),
                properties,
            )
            .await
            .map_err(AmqpError::publish)?;
        Ok(())
    }
}

impl RequestReply for LapinRequester {
    type Reply = LapinMessage;

    /// Sends `msg` and awaits the correlated reply.
    ///
    /// # Errors
    ///
    /// Returns [`AmqpError::RequestTimeout`] when no reply arrives within `timeout`,
    /// [`AmqpError::NotConnected`] before `Broker::connect` resolves the connection, and
    /// [`AmqpError::Request`] / [`AmqpError::Publish`] on channel failures.
    ///
    /// # Cancel safety
    ///
    /// Cancel safe for the caller's state: dropping the future abandons the pending slot and a
    /// late reply is discarded. The request itself may still have been published.
    async fn request(
        &self,
        msg: OutgoingMessage<'_>,
        timeout: Duration,
    ) -> Result<Self::Reply, Self::Error> {
        let state = self.state().await?;

        let correlation_id = format!("rs-{}", self.next_id.fetch_add(1, Ordering::Relaxed));
        let (tx, rx) = oneshot::channel();
        {
            let mut pending = self
                .pending
                .lock()
                .expect("pending requests mutex poisoned");
            pending.insert(correlation_id.clone(), tx);
        }
        // Every failure path below must reclaim the slot, or it leaks until shutdown.
        let cleanup = || {
            let mut pending = self
                .pending
                .lock()
                .expect("pending requests mutex poisoned");
            pending.remove(&correlation_id);
        };

        let properties = match convert::properties_for_publish(msg.headers(), self.persistent) {
            Ok(properties) => properties
                .with_reply_to(ShortString::from(REPLY_TO))
                .with_correlation_id(ShortString::from(correlation_id.clone())),
            Err(err) => {
                cleanup();
                return Err(err);
            }
        };
        let exchange = match convert::short(&self.exchange, "exchange name") {
            Ok(exchange) => exchange,
            Err(err) => {
                cleanup();
                return Err(err);
            }
        };
        let routing_key = match convert::short(msg.name(), "routing key") {
            Ok(routing_key) => routing_key,
            Err(err) => {
                cleanup();
                return Err(err);
            }
        };

        let published = state
            .channel
            .basic_publish(
                exchange,
                routing_key,
                BasicPublishOptions::default(),
                msg.payload(),
                properties,
            )
            .await;
        if let Err(err) = published {
            cleanup();
            return Err(AmqpError::publish(err));
        }

        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(reply)) => Ok(reply),
            // The dispatch task dropped the sender: the reply channel died under us.
            Ok(Err(_)) => {
                cleanup();
                Err(AmqpError::Request(
                    "the reply consumer stopped before a reply arrived".into(),
                ))
            }
            Err(_) => {
                cleanup();
                Err(AmqpError::RequestTimeout(timeout))
            }
        }
    }
}
