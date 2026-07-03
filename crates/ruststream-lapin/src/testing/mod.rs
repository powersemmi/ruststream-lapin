//! In-process test broker, behind the `testing` feature.
//!
//! [`LapinTestBroker`] implements the core `TestableBroker` contract over an in-memory router
//! so application handlers wired against `RabbitMQ` descriptors can be exercised without a
//! server: messages fan out synchronously to subscribers matched by exact queue name.
//!
//! Scope: queue-name routing, settlement, headers, and buffered transactions. Exchange types,
//! bindings, dead-lettering, prefetch, and request/reply are transport behavior; exercise them
//! against a real server (see the crate's integration tests and `AMQP_TEST_URL`).

mod broker;
mod publisher;
mod router;
mod subscriber;

pub use broker::LapinTestBroker;
pub use publisher::LapinTestPublisher;
pub use subscriber::{LapinTestMessage, LapinTestSubscriber};
