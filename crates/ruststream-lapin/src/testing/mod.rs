//! In-process test broker, behind the `testing` feature.
//!
//! The broker follows the same ladder as the real one (synchronous `new`, consuming `connect`,
//! consuming `shutdown`) over an in-memory transport, so application handlers wired against
//! `RabbitMQ` descriptors can be exercised without a server: a message reaches the queue its
//! routing key names, and one consumer of that queue receives it. Publishing keeps the crate's
//! production policies - [`LapinPublish`](crate::LapinPublish),
//! [`ConfirmsPublish`](crate::ConfirmsPublish), [`ServerTxPublish`](crate::ServerTxPublish) and
//! [`LapinRequest`](crate::LapinRequest) all pair here - so a routes file mounts unchanged.
//! Public surface:
//!
//! * [`LapinTestBroker`] / [`ConnectedLapinTestBroker`] - the ladder; the connected form
//!   implements [`TestableBroker`](ruststream::testing::TestableBroker), so it drives both the
//!   [`TestApp`](ruststream::testing::TestApp) harness and the framework's conformance suite;
//! * [`LapinTestPublishPolicy`] - what a production policy pairs into here, one stand-in per live
//!   publisher: [`LapinTestPublisher`], [`ConfirmsTestPublisher`] (with the owned
//!   [`ConfirmsTestTransaction`]), [`ServerTxTestPublisher`] and [`LapinTestRequester`], each
//!   carrying exactly the capabilities its counterpart carries;
//! * [`LapinTestSubscriber`] / [`LapinTestMessage`] - the `Subscriber` and `IncomingMessage`
//!   impls, settling like the real transport.
//!
//! Scope: queue-name routing, competing consumers, settlement and redelivery, headers, buffered
//! transactions, and request/reply correlation. What the transport cannot reproduce is stated on
//! the type that would otherwise imply it, and collected in the crate's testing guide: the
//! server-side halves of exchanges and bindings, dead-lettering, prefetch, publisher confirms,
//! AMQP server transactions, and the at-most-once nature of direct reply-to. Exercise those
//! against a real server (see the crate's integration tests and `AMQP_TEST_URL`).

mod broker;
mod publisher;
mod requester;
mod router;
mod subscriber;

pub use broker::{ConnectedLapinTestBroker, LapinTestBroker};
pub use publisher::{
    ConfirmsTestPublisher, ConfirmsTestTransaction, LapinTestPublishPolicy, LapinTestPublisher,
    ServerTxTestPublisher,
};
pub use requester::LapinTestRequester;
pub use subscriber::{LapinTestMessage, LapinTestSubscriber};
