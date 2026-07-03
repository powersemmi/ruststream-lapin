//! `RabbitMQ` / AMQP 0.9.1 broker for the
//! [RustStream](https://github.com/powersemmi/ruststream) messaging framework, backed by
//! [`lapin`].
//!
//! # Transport model
//!
//! A subscription consumes one queue; [`RabbitQueue`] describes the queue and its bindings, and
//! the bare-string `#[subscriber("orders")]` form consumes the queue named `orders`. On the
//! publish side [`OutgoingMessage::name`](ruststream::OutgoingMessage) is the routing key, sent
//! to the publisher's exchange (the default exchange unless configured, where the routing key
//! addresses the queue with that name).
//!
//! Settlement uses the protocol natively, with no republish tricks:
//!
//! - ack sends `basic.ack`
//! - retry (`nack(true)`) sends `basic.nack` with requeue
//! - drop (`nack(false)`) sends `basic.reject` without requeue, dead-lettering when the queue
//!   has a dead-letter exchange
//!
//! # Lazy startup
//!
//! [`LapinBroker::new`] is synchronous and I/O-free, so a service composes with the synchronous
//! `#[ruststream::app]` builder; the real network work happens in the idempotent async
//! `Broker::connect`, called once by the runtime at startup. Publishers handed out before that
//! resolve the shared connection on first use.
//!
//! # Topology
//!
//! Descriptors describe the EXPECTED topology; nothing is created on the broker by default,
//! because managing infrastructure is the user's job. Declaration is a per-broker opt-in:
//! [`LapinBroker::declare_topology`].
//!
//! [`lapin`]: https://docs.rs/lapin

#![forbid(unsafe_code)]

mod broker;
mod convert;
mod error;
mod exchange;
mod message;
mod publisher;
mod queue;
mod requester;
mod subscriber;

pub mod context;
#[cfg(feature = "testing")]
pub mod testing;

pub use broker::LapinBroker;
pub use error::AmqpError;
pub use exchange::RabbitExchange;
pub use message::LapinMessage;
pub use publisher::{ConfirmsPublisher, LapinPublisher, ServerTxPublisher};
pub use queue::{QueueType, RabbitQueue};
pub use requester::LapinRequester;
pub use subscriber::LapinSubscriber;

// Raw declaration-argument passthrough (`RabbitQueue::argument` / `arguments`).
pub use lapin::types::{AMQPValue, FieldTable};
