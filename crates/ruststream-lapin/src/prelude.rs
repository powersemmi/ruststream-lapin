//! The imports a service on `RabbitMQ` writes every time, in one glob.
//!
//! `use ruststream_lapin::prelude::*;` brings the framework's own prelude with it, and then this
//! broker's vocabulary: the broker itself, the queue and exchange descriptors, every publish
//! policy mode, the per-message publish steps, the direct reply-to transform, and the delivery
//! metadata keys a handler reads by name. One import, and a service file is ready to declare its
//! subscriptions and its publishers.
//!
//! It is also a capability manifest. The glob carries the framework capabilities this broker's
//! live forms implement and a service names for itself - transactions in both shapes, settling an
//! owned one, request/reply - and nothing it does not: a handler that asks for a capability AMQP
//! does not have cannot even name it here. A service that speaks to more than one broker globs
//! more than one prelude: the capability traits are the framework's own, so the globs agree on
//! them by construction, and the compiler says so.
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

// The framework's prelude stops short of brokers, because which broker a service runs on is the
// one thing every service states for itself. Importing this prelude is that statement: the
// broker is named by the crate path the glob comes from, so the framework's own prelude rides
// along and one import serves the file.
pub use ruststream::prelude::*;

// The capability half of the manifest: the framework capabilities this broker's live forms
// implement, and only those - the ones a service writes in a bound, plus the ones whose methods
// it calls on a value it is handed. `Transaction` is the second kind and easy to miss: without
// it here, the value `OwnedTransactions::transaction` hands back could not be settled from the
// glob. AMQP has no cursor, so nothing seekable or positioned is here, and batching stays a
// client-side concern.
pub use ruststream::{OwnedTransactions, RequestReply, Transaction, TransactionalPublisher};

pub use crate::context::keys::{DeliveryTag, Exchange, Redelivered, RoutingKey};
pub use crate::{
    AMQPValue, ConfirmsPublish, Delay, DirectReplyTo, FieldTable, LapinBroker, LapinPublish,
    LapinPublishExt, LapinRequest, QueueType, RabbitExchange, RabbitQueue, ServerTxPublish,
};

// Deliberately absent, each for its own reason:
//
// The `testing` module, which is feature-gated: a test names the broker it fakes explicitly, the
// same way a service names the broker it runs on.
//
// The live forms the wiring produces - the connected and closed broker, the paired publishers
// and requester, the subscriber, the delivery, the transaction and step values - for the reason
// the framework's prelude gives for leaving `OutgoingMessage` out: a service receives them, it
// does not write them, and the code that does name one says by that import which layer it works
// at.
//
// `Partitioned`, which the delivery implements: `IncomingMessage::partition_key` is a defaulted
// method the framework's prelude already carries, and the delivery's impl is what backs it, so
// re-exporting the trait here would make the natural `msg.partition_key()` call ambiguous.
//
// `Subscribe`, `DefaultPublish` and `DescribeServer`, which this crate also implements: they are
// the contract between a broker and the runtime, and the runtime is the only caller.
//
// `AmqpError`: a service names errors where it handles them.
