//! The imports a service on `RabbitMQ` writes every time, in one glob.
//!
//! # Examples
//!
//! ```
//! use ruststream_lapin::prelude::*;
//!
//! let broker = LapinBroker::new("amqp://localhost:5672");
//! let orders = RabbitQueue::new("orders").durable(true);
//! let shipments = LapinPublish::default().exchange("shipments").confirms();
//! # let _ = (broker, orders, shipments);
//! ```
//!
//! The framework's own prelude comes with it, so one import serves the file. Every name this
//! crate adds keeps its `Lapin` / `Rabbit` prefix, so a service that speaks to more than one
//! broker globs each broker's prelude and nothing collides. The prefix is also what keeps the
//! framework's own vocabulary reachable: the bare `Publish` is its slot capability trait (what a
//! hand-written handler bounds an injected publisher with), and an unprefixed re-export here
//! would shadow it silently for every file globbing this module.

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

// `Partitioned` stays out although the delivery implements it: `IncomingMessage::partition_key`
// is a defaulted method the framework's prelude already carries, and re-exporting the trait makes
// `msg.partition_key()` ambiguous.
