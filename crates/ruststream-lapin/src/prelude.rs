//! The imports a service on `RabbitMQ` writes every time, in one glob.
//!
//! # Examples
//!
//! ```
//! use ruststream_lapin::prelude::*;
//!
//! let broker = LapinBroker::new("amqp://localhost:5672");
//! let orders = RabbitQueue::new("orders").durable(true);
//! let shipments = Publish::default().exchange("shipments").confirms();
//! # let _ = (broker, orders, shipments);
//! ```
//!
//! The framework's own prelude comes with it, so one import serves the file. A service that
//! speaks to more than one broker globs each broker's prelude and names the prefixed originals
//! ([`LapinPublish`], [`LapinRequest`]) where two of them meet.

pub use ruststream::prelude::*;

// `Transaction` is here for the value `OwnedTransactions::transaction` hands back: without it,
// that value cannot be settled from the glob.
pub use ruststream::{OwnedTransactions, RequestReply, Transaction, TransactionalPublisher};

pub use crate::context::keys::{DeliveryTag, Exchange, Redelivered, RoutingKey};
// `ConfirmsPublish` and `ServerTxPublish` keep separate names: different guarantees, not two
// spellings of one policy.
pub use crate::{
    AMQPValue, ConfirmsPublish, Delay, DirectReplyTo, FieldTable, LapinBroker, LapinPublish,
    LapinPublishExt, LapinRequest, QueueType, RabbitExchange, RabbitQueue, ServerTxPublish,
};

/// The policy names an include site writes: `Publish` is [`LapinPublish`], `Request` is
/// [`LapinRequest`].
///
/// `Publish` is the publish policy, not the framework's `runtime::Publish` builder.
pub use crate::{LapinPublish as Publish, LapinRequest as Request};

// `Partitioned` stays out although the delivery implements it: `IncomingMessage::partition_key`
// is a defaulted method the framework's prelude already carries, and re-exporting the trait makes
// `msg.partition_key()` ambiguous.
