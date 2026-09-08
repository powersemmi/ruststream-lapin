//! The imports a service on `RabbitMQ` writes every time, in one glob.
//!
//! # Examples
//!
//! ```
//! use ruststream_lapin::prelude::*;
//!
//! let broker = LapinBroker::new("amqp://localhost:5672");
//! let orders = RabbitQueue::new("orders").durable(true);
//! let shipments = TransactionalPublish::default().exchange("shipments");
//! # let _ = (broker, orders, shipments);
//! ```
//!
//! A service writes two kinds of file, and each names a different thing.
//!
//! A handler body names CAPABILITIES: it imports the framework's prelude alone and bounds an
//! injected publisher with the capability trait it needs (`Out<impl Publisher>`,
//! `Out<impl TransactionalPublisher>`, `Out<impl RequestReply>`), so no broker type reaches the
//! signature and the same handler mounts on a real broker or on its in-process transport. A
//! routes file names VALUES, and imports this prelude: the framework's own comes with it, and on
//! top of that every broker in the family spells its mount-site policies the same way -
//! `Publish`, `TransactionalPublish`, `Request` - so a router reads the same whichever broker it
//! is written against. A service that speaks to more than one broker keeps a routes file per
//! broker and names the prefixed originals ([`LapinPublish`], [`ConfirmsPublish`],
//! [`LapinRequest`]) where two of them meet.

pub use ruststream::prelude::*;

// `Transaction` is here for the value `OwnedTransactions::transaction` hands back: without it,
// that value cannot be settled from the glob.
pub use ruststream::{OwnedTransactions, RequestReply, Transaction, TransactionalPublisher};

pub use crate::context::keys::{DeliveryTag, Exchange, Redelivered, RoutingKey};
pub use crate::{
    AMQPValue, ConfirmsPublish, Delay, DirectReplyTo, FieldTable, LapinBroker, LapinPublish,
    LapinPublishExt, LapinRequest, QueueType, RabbitExchange, RabbitQueue, ServerTxPublish,
};

/// The policy names a mount site writes, uniform across the broker family: `Publish` is
/// [`LapinPublish`], `TransactionalPublish` is [`ConfirmsPublish`], `Request` is
/// [`LapinRequest`].
///
/// `TransactionalPublish` is the confirms policy, not the plain one. On AMQP the publishing mode
/// is a policy type transition rather than a flag: [`LapinPublish`] pairs into a publisher with
/// no transaction surface at all, and [`confirms`](LapinPublish::confirms) is what moves to the
/// one carrying both transaction shapes (the borrowed `TransactionalPublisher` and the owned
/// `OwnedTransactions`). So a handler bounded `Out<impl TransactionalPublisher>` mounts against
/// this name and against nothing weaker, which is the compile-time check the transition exists
/// for.
///
/// [`ServerTxPublish`] keeps its own name: AMQP server transactions are a different guarantee
/// (atomic visibility at commit, a synchronous round trip to pay for it), not a second spelling
/// of this one.
pub use crate::{
    ConfirmsPublish as TransactionalPublish, LapinPublish as Publish, LapinRequest as Request,
};

// `Partitioned` stays out although the delivery implements it: `IncomingMessage::partition_key`
// is a defaulted method the framework's prelude already carries, and re-exporting the trait makes
// `msg.partition_key()` ambiguous.
