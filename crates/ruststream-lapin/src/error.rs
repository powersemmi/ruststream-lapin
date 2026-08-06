//! The crate error type shared by broker, publishers, subscribers, and the requester.

use std::error::Error as StdError;
use std::time::Duration;

use thiserror::Error;

/// Errors returned by [`LapinBroker`](crate::LapinBroker) and the types it hands out.
///
/// Underlying [`lapin`](https://docs.rs/lapin) errors are boxed as sources so the client library
/// does not leak into this crate's public API surface.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum AmqpError {
    /// Establishing or closing the connection failed.
    #[error("amqp connection error: {0}")]
    Connect(#[source] Box<dyn StdError + Send + Sync>),

    /// Publishing a message failed, or the broker refused to confirm it.
    #[error("amqp publish error: {0}")]
    Publish(#[source] Box<dyn StdError + Send + Sync>),

    /// Opening a subscription (channel, `QoS`, or consume) failed.
    #[error("amqp subscribe error: {0}")]
    Subscribe(#[source] Box<dyn StdError + Send + Sync>),

    /// Receiving a delivery from an open consumer failed.
    #[error("amqp consume error: {0}")]
    Consume(#[source] Box<dyn StdError + Send + Sync>),

    /// Declaring the expected topology (exchange, queue, or binding) failed.
    #[error("amqp topology declaration error: {0}")]
    Declare(#[source] Box<dyn StdError + Send + Sync>),

    /// Sending a request or receiving its reply failed.
    #[error("amqp request error: {0}")]
    Request(#[source] Box<dyn StdError + Send + Sync>),

    /// No reply arrived within the caller's deadline.
    ///
    /// The pending request is dropped; a reply arriving later is discarded.
    #[error("amqp request timed out after {0:?} without a reply")]
    RequestTimeout(Duration),

    /// An operation ran against a connection that has already shut down.
    ///
    /// The ladder makes misuse through the owner of the connected broker a compile error, so
    /// this reports the case it cannot cover: a handle aliasing the connection (a publisher
    /// paired before the shutdown, a requester clone) used afterwards, which must fail rather
    /// than silently succeed against a dead connection.
    #[error("amqp connection is closed; the operation targeting {target:?} cannot proceed")]
    Closed {
        /// The routing key, queue, or exchange the operation targeted.
        target: String,
    },

    /// A transaction call ran out of order on a transactional publisher.
    ///
    /// A commit or an abort with no open transaction, or a second begin while one is open. The
    /// message names the offending call.
    #[error("invalid transaction state: {0}")]
    Transaction(String),

    /// The requested combination of options cannot be executed.
    ///
    /// The message names the offending option and the remediation.
    #[error("invalid options: {0}")]
    InvalidOptions(String),
}

impl AmqpError {
    pub(crate) fn connect(err: lapin::Error) -> Self {
        Self::Connect(Box::new(err))
    }

    pub(crate) fn publish(err: lapin::Error) -> Self {
        Self::Publish(Box::new(err))
    }

    pub(crate) fn subscribe(err: lapin::Error) -> Self {
        Self::Subscribe(Box::new(err))
    }

    pub(crate) fn consume(err: lapin::Error) -> Self {
        Self::Consume(Box::new(err))
    }

    pub(crate) fn declare(err: lapin::Error) -> Self {
        Self::Declare(Box::new(err))
    }

    pub(crate) fn request(err: lapin::Error) -> Self {
        Self::Request(Box::new(err))
    }

    pub(crate) fn closed(target: &str) -> Self {
        Self::Closed {
            target: target.to_owned(),
        }
    }
}
