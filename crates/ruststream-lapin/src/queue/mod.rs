//! The queue descriptors: what a subscription binds to and, optionally, expects to exist.
//!
//! `RabbitMQ` has two queue implementations and they answer a retry differently, so each has a
//! descriptor of its own. [`RabbitQueue`] is the classic queue: it counts nothing, so a
//! registration's cap is the runtime's to apply with the copies it publishes. [`RabbitQuorumQueue`]
//! counts the deliveries a message spends and carries a spent one away itself, so the same cap
//! becomes the queue's own arguments and no copy is published at all.

use ruststream::RetryDeclaration;

use crate::error::AmqpError;

mod classic;
mod quorum;
mod spec;

pub use classic::RabbitQueue;
pub use quorum::RabbitQuorumQueue;
pub use spec::QueueSpec;
pub(crate) use spec::{DEAD_LETTER_EXCHANGE, DEAD_LETTER_ROUTING_KEY, declared_arguments};
// The in-process transport reads a declaration's arguments back the way a server reads them off
// the queue, and nothing else in the crate needs them by these names.
#[cfg(feature = "testing")]
pub(crate) use spec::{DELIVERY_LIMIT, QueueKind};

mod sealed {
    /// Closes the list of queue descriptors: `RabbitMQ` has two queue implementations, and which
    /// one a subscription consumes decides what a retry does.
    pub trait Sealed {}

    impl Sealed for super::RabbitQueue {}
    impl Sealed for super::RabbitQuorumQueue {}
}

/// The queue a subscription consumes: [`RabbitQueue`] or [`RabbitQuorumQueue`].
///
/// Sealed. A queue's implementation is fixed when the queue is created, and what a retry does
/// follows from it, so the two descriptors are the whole list. The methods are how the broker
/// reads a descriptor; they are machinery, never called directly.
pub trait QueueDescriptor: sealed::Sealed {
    /// The settings the queue is declared and consumed with.
    #[doc(hidden)]
    fn spec(&self) -> &QueueSpec;

    /// What the mount site declared about this registration's retries, where the queue is the one
    /// that applies it. `None` on a queue whose retries the runtime applies.
    #[doc(hidden)]
    fn declared_retry(&self) -> Option<&RetryDeclaration>;

    /// Refuses a declaration this queue cannot carry, before the subscription opens.
    ///
    /// # Errors
    ///
    /// Returns [`AmqpError::InvalidOptions`] when the registration declared half a retry policy,
    /// or a whole one on a queue this service does not declare.
    #[doc(hidden)]
    fn check_retry(&self, declares_topology: bool) -> Result<(), AmqpError>;
}

impl QueueDescriptor for RabbitQueue {
    fn spec(&self) -> &QueueSpec {
        self.spec()
    }

    /// A classic queue counts nothing and has no delivery limit, so the declaration stays with
    /// the runtime and reaches no queue argument.
    fn declared_retry(&self) -> Option<&RetryDeclaration> {
        None
    }

    fn check_retry(&self, _declares_topology: bool) -> Result<(), AmqpError> {
        Ok(())
    }
}

impl QueueDescriptor for RabbitQuorumQueue {
    fn spec(&self) -> &QueueSpec {
        self.spec()
    }

    fn declared_retry(&self) -> Option<&RetryDeclaration> {
        Some(self.retry())
    }

    fn check_retry(&self, declares_topology: bool) -> Result<(), AmqpError> {
        self.check_retry(declares_topology)
    }
}
