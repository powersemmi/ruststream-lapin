#![doc = include_str!("README.md")]
#![forbid(unsafe_code)]

#[cfg(feature = "asyncapi")]
mod bindings;
mod broker;
mod channel;
mod convert;
mod delay;
mod error;
mod exchange;
mod message;
mod publish_policy;
mod publish_step;
mod publisher;
mod queue;
mod reply;
mod requester;
mod subscriber;
mod topology;
mod transaction;

pub mod context;
pub mod prelude;
#[cfg(feature = "testing")]
pub mod testing;

pub use broker::{ClosedLapinBroker, ConnectedLapinBroker, LapinBroker};
pub use delay::Delay;
pub use error::AmqpError;
pub use exchange::RabbitExchange;
pub use message::{LapinMessage, PARTITION_KEY_HEADER};
pub use publish_policy::{ConfirmsPublish, LapinPublish, LapinPublishPolicy, ServerTxPublish};
pub use publish_step::{
    EXPIRATION_HEADER, LapinPublishOptions, LapinPublishSteps, PRIORITY_HEADER,
};
pub use publisher::{ConfirmsPublisher, LapinPublisher, ServerTxPublisher};
pub use queue::{QueueDescriptor, QueueSpec, RabbitQueue, RabbitQuorumQueue};
pub use reply::DirectReplyTo;
pub use requester::{LapinRequest, LapinRequester};
pub use subscriber::LapinSubscriber;
pub use transaction::ConfirmsTransaction;

// Raw declaration-argument passthrough (`RabbitQueue::argument` / `arguments`).
pub use lapin::types::{AMQPValue, FieldTable};
