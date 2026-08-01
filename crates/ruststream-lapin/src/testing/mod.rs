//! In-process test broker, behind the `testing` feature.
//!
//! The broker follows the same ladder as the real one (synchronous `new`, consuming `connect`,
//! consuming `shutdown`) over an in-memory router, so application handlers wired against
//! `RabbitMQ` descriptors can be exercised without a server: messages fan out synchronously to
//! subscribers matched by exact queue name. Public surface:
//!
//! * [`LapinTestBroker`] / [`ConnectedLapinTestBroker`] - the ladder; the connected form
//!   implements [`TestableBroker`](ruststream::testing::TestableBroker), so it drives both the
//!   [`TestApp`](ruststream::testing::TestApp) harness and the framework's conformance suite;
//! * [`LapinTestPublish`] / [`LapinTestPublisher`] - the publish pair, with buffered transactions;
//! * [`LapinTestSubscriber`] / [`LapinTestMessage`] - the `Subscriber` and `IncomingMessage`
//!   impls, settling like the real transport.
//!
//! Scope: queue-name routing, settlement, headers, and buffered transactions. Exchange types,
//! bindings, dead-lettering, prefetch, and request/reply are transport behavior; exercise them
//! against a real server (see the crate's integration tests and `AMQP_TEST_URL`).

mod broker;
mod publisher;
mod router;
mod subscriber;

pub use broker::{ConnectedLapinTestBroker, LapinTestBroker};
pub use publisher::{LapinTestPublish, LapinTestPublisher};
pub use subscriber::{LapinTestMessage, LapinTestSubscriber};
