//! Keeps consumer polling alive while Lapin restores its connection and topology.
//!
//! Subscribers and RPC reply dispatchers share [`RecoveringConsumer`]. With recovery enabled,
//! recoverable errors pause polling until Lapin restores the channel; a terminal error is yielded
//! once and ends the stream. Shutdown wakes blocked consumers without waiting for reconnection.
//! This layer never replays publishes or renews RPC deadlines.
//!
//! Recovery is opt-in through [`LapinBroker::auto_recover`](crate::LapinBroker::auto_recover).
//! Applications keep their existing subscription and let its stream resume after an outage:
//!
//! ```no_run
//! use futures::StreamExt;
//! use ruststream::{Broker, ConnectedBroker, IncomingMessage, Subscriber};
//! use ruststream_lapin::{LapinBroker, RabbitQueue};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let broker = LapinBroker::new("amqp://localhost:5672?heartbeat=30")
//!     .auto_recover(true)
//!     .declare_topology(true)
//!     .connect()
//!     .await?;
//! let mut subscriber = broker.subscribe(RabbitQueue::new("orders")).await?;
//! let mut messages = Box::pin(subscriber.stream());
//! while let Some(message) = messages.next().await {
//!     let message = message?;
//!     // Process the delivery before acknowledging it; redelivery is possible after an outage.
//!     message.ack().await?;
//! }
//! drop(messages);
//! broker.shutdown().await?;
//! # Ok(())
//! # }
//! ```
//!
//! A readiness endpoint can inspect the connection without triggering a restart:
//!
//! ```no_run
//! use ruststream::Broker;
//! use ruststream_lapin::{AmqpError, LapinBroker};
//!
//! # async fn example() -> Result<(), AmqpError> {
//! let broker = LapinBroker::new("amqp://localhost:5672")
//!     .auto_recover(true)
//!     .connect()
//!     .await?;
//! let ready = broker.is_connected();
//! // `ready` checks the AMQP connection; topology replay and worker health are separate.
//! # let _ = ready;
//! # Ok(())
//! # }
//! ```

use std::sync::Arc;

use futures::StreamExt;
use lapin::message::Delivery;
use lapin::{Channel, ConnectionState, Consumer, ErrorKind};
use tokio::sync::watch;

use crate::broker::AmqpConnection;
use crate::error::AmqpError;

/// A native consumer whose polling survives recoverable connection errors.
///
/// Owns the dedicated channel so dropping a live subscription still cancels it. The connection's
/// notification wakes idle polling when shutdown starts or reconnect attempts are exhausted,
/// including failures for which Lapin leaves its recovery future pending.
pub(crate) struct RecoveringConsumer {
    /// Shared with deliveries so settlement can reject operations after broker shutdown.
    pub(crate) connection: Arc<AmqpConnection>,
    /// The original channel; Lapin restores it rather than replacing this handle.
    channel: Channel,
    /// The same consumer is restored by Lapin's topology recovery.
    consumer: Consumer,
    /// Becomes true on shutdown or terminal connection failure.
    recovery: watch::Receiver<bool>,
    /// Prevents another poll or repeated errors after the consumer has terminated.
    ended: bool,
}

impl Drop for RecoveringConsumer {
    fn drop(&mut self) {
        if self.ended && !self.channel.status().connected() {
            // shortcut: suppress cancellation of a removed consumer; remove when Lapin fixes drop after channel errors.
            self.consumer.set_delegate(|_| async {});
        }
    }
}

impl RecoveringConsumer {
    /// Wraps an already-open consumer and subscribes to its connection's terminal notifications.
    ///
    /// Performs no I/O and does not enable recovery; the broker sets that policy when connecting.
    pub(crate) fn new(
        connection: Arc<AmqpConnection>,
        channel: Channel,
        consumer: Consumer,
    ) -> Self {
        let recovery = connection.recovery();
        Self {
            connection,
            channel,
            consumer,
            recovery,
            ended: false,
        }
    }

    /// Waits for Lapin to restore the channel, while also observing shutdown and retry exhaustion.
    ///
    /// Returns the consume error when recovery is unavailable or the connection terminates.
    async fn recover(&mut self, error: lapin::Error) -> Result<(), AmqpError> {
        let recovery = self.channel.wait_for_recovery(error.clone());
        tokio::pin!(recovery);
        loop {
            if self.connection.is_closed() || self.connection.failed() {
                return Err(AmqpError::consume(error));
            }
            tokio::select! {
                biased;
                changed = self.recovery.changed() => {
                    if changed.is_err() { return Err(AmqpError::consume(error)); }
                }
                result = &mut recovery => return result.map_err(AmqpError::consume),
            }
        }
    }

    /// Receives one delivery, waiting through recoverable errors without ending the stream.
    ///
    /// A terminal failure yields one [`AmqpError::Consume`], then subsequent calls return `None`.
    /// Consumer cancellation and broker shutdown end polling. No deliveries are acknowledged here.
    ///
    /// # Cancel safety
    ///
    /// Dropping this future leaves the consumer and recovery subscription intact. A later call
    /// resumes polling, so batch deadlines can interrupt a recovery wait safely.
    pub(crate) async fn next(&mut self) -> Option<Result<Delivery, AmqpError>> {
        loop {
            if self.ended || self.connection.is_closed() {
                return None;
            }
            if *self.recovery.borrow() {
                self.ended = true;
                return Some(Err(AmqpError::consume(
                    ErrorKind::InvalidConnectionState(ConnectionState::Error).into(),
                )));
            }
            let item = tokio::select! {
                biased;
                changed = self.recovery.changed() => {
                    if changed.is_err() { self.ended = true; return None; }
                    continue;
                }
                item = self.consumer.next() => item,
            };
            match item {
                Some(Err(error)) => {
                    if let Err(error) = self.recover(error).await {
                        self.ended = true;
                        return Some(Err(error));
                    }
                }
                None => {
                    self.ended = true;
                    return None;
                }
                Some(Ok(delivery)) => return Some(Ok(delivery)),
            }
        }
    }
}
