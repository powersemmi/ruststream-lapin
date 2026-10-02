//! The imports a service on `RabbitMQ` writes every time, in one glob.
//!
//! # Examples
//!
//! ```
//! use ruststream_lapin::prelude::*;
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Deserialize)]
//! struct Order {
//!     id: u64,
//! }
//!
//! #[derive(Serialize, Outgoing)]
//! #[outgoing(name = "shipment.requested")]
//! struct Shipment {
//!     order_id: u64,
//! }
//!
//! #[subscriber(RabbitQueue::new("orders").durable(true))]
//! async fn ship(order: &Order, Out(shipments): Out<impl TransactionalPublisher>) -> HandlerOutcome {
//!     let shipment = Shipment { order_id: order.id };
//!     if shipments.message(&shipment).publish().await.is_err() {
//!         return HandlerOutcome::retry();
//!     }
//!     HandlerOutcome::ack()
//! }
//!
//! fn app() -> RustStream {
//!     let broker = LapinBroker::new("amqp://localhost:5672");
//!     let shipments = TransactionalPublish::default().exchange("shipments");
//!     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
//!         b.include(ship).out(DefaultSlot, shipments).build();
//!     })
//! }
//! ```
//!
//! A service writes two kinds of file, and each names a different thing.
//!
//! A handler body names CAPABILITIES: it imports the framework's prelude alone and bounds an
//! injected publisher with the capability trait it needs (`Out<impl Publisher>`,
//! `Out<impl TransactionalPublisher>`, `Out<impl RequestReply>`), so no broker type reaches the
//! signature and the same handler mounts under any policy the routes file pairs it with. A
//! routes file names VALUES, and imports this prelude: the framework's own comes with it, and on
//! top of that every broker in the family spells its mount-site policies the same way -
//! `Publish`, `TransactionalPublish`, `Request` - so a router reads the same whichever broker it
//! is written against. A service that speaks to more than one broker keeps a routes file per
//! broker and names the prefixed originals ([`LapinPublish`], [`ConfirmsPublish`],
//! [`LapinRequest`]) where two of them meet.
//!
//! One kind of handler body does import this prelude: one that adjusts an AMQP property for a
//! single message. The steps of [`LapinPublishSteps`] come from here, and the slot they run on is
//! bounded `Out<impl Publisher<Options = LapinPublishOptions>, Marker>` - the options type is what
//! the compiler matches, so the body still names no publisher type.

pub use ruststream::prelude::*;

// `Transaction` is here for the value `OwnedTransactions::transaction` hands back: without it,
// that value cannot be settled from the glob.
pub use ruststream::{OwnedTransactions, RequestReply, Transaction, TransactionalPublisher};

pub use crate::context::keys::{DeliveryTag, Exchange, Redelivered, RoutingKey};
pub use crate::{
    AMQPValue, ConfirmsPublish, Delay, DirectReplyTo, FieldTable, LapinBroker, LapinPublish,
    LapinPublishOptions, LapinPublishSteps, LapinRequest, RabbitExchange, RabbitQueue,
    RabbitQuorumQueue, ServerTxPublish,
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
