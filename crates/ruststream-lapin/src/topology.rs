//! Opt-in topology declaration: exchanges, queues, bindings, and the delay backend.
//!
//! Only reached when the broker opted into
//! [`declare_topology`](crate::LapinBroker::declare_topology); otherwise a descriptor is a
//! statement about infrastructure that must already exist.

use lapin::Channel;
use lapin::options::{ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions};
use lapin::types::{AMQPValue, FieldTable, ShortString};

use crate::convert;
use crate::delay::{Delay, DelayTarget};
use crate::error::AmqpError;
use crate::queue::{DEAD_LETTER_EXCHANGE, DEAD_LETTER_ROUTING_KEY, QueueSpec};

/// Declares the exchanges, queue, bindings, and delay backend `spec` describes, with `arguments`
/// as the descriptor and the registration between them asked for.
pub(crate) async fn declare(
    channel: &Channel,
    spec: &QueueSpec,
    arguments: FieldTable,
) -> Result<(), AmqpError> {
    for binding in &spec.bindings {
        let exchange = &binding.exchange;
        // The default exchange and the amq.* built-ins exist on every broker and must not be
        // redeclared.
        if exchange.name().is_empty() || exchange.name().starts_with("amq.") {
            continue;
        }
        channel
            .exchange_declare(
                convert::short(exchange.name(), "exchange name")?,
                exchange.kind().clone(),
                ExchangeDeclareOptions {
                    durable: exchange.is_durable(),
                    auto_delete: exchange.is_auto_delete(),
                    ..ExchangeDeclareOptions::default()
                },
                FieldTable::default(),
            )
            .await
            .map_err(AmqpError::declare)?;
    }

    channel
        .queue_declare(
            convert::short(&spec.name, "queue name")?,
            QueueDeclareOptions {
                durable: spec.durable,
                exclusive: spec.exclusive,
                auto_delete: spec.auto_delete,
                ..QueueDeclareOptions::default()
            },
            arguments,
        )
        .await
        .map_err(AmqpError::declare)?;

    for binding in &spec.bindings {
        channel
            .queue_bind(
                convert::short(&spec.name, "queue name")?,
                convert::short(binding.exchange.name(), "exchange name")?,
                convert::short(&binding.routing_key, "routing key")?,
                QueueBindOptions::default(),
                binding.arguments.clone(),
            )
            .await
            .map_err(AmqpError::declare)?;
    }

    if let Some(delay) = &spec.delay {
        declare_delay_backend(channel, delay, &spec.name).await?;
    }

    Ok(())
}

/// Declares the infrastructure the delay backend needs to route a delayed copy back to `origin`.
async fn declare_delay_backend(
    channel: &Channel,
    delay: &Delay,
    origin: &str,
) -> Result<(), AmqpError> {
    match delay.target_for(origin) {
        DelayTarget::WaitingQueue { waiting_queue } => {
            declare_delay_queue(channel, &waiting_queue, origin).await
        }
        #[cfg(feature = "plugin-dme")]
        DelayTarget::DelayedExchange {
            exchange,
            routing_key,
        } => declare_delayed_exchange(channel, &exchange, origin, &routing_key).await,
    }
}

/// Declares the delay waiting queue: durable, with a per-message TTL applied by the sender and a
/// dead-letter route back to `origin` on the default exchange (so an expired message returns to
/// the queue it came from).
async fn declare_delay_queue(
    channel: &Channel,
    waiting_queue: &str,
    origin: &str,
) -> Result<(), AmqpError> {
    let mut arguments = FieldTable::default();
    arguments.insert(
        ShortString::from(DEAD_LETTER_EXCHANGE),
        AMQPValue::LongString(String::new().into()),
    );
    arguments.insert(
        ShortString::from(DEAD_LETTER_ROUTING_KEY),
        AMQPValue::LongString(origin.into()),
    );
    channel
        .queue_declare(
            convert::short(waiting_queue, "waiting queue name")?,
            QueueDeclareOptions {
                durable: true,
                ..QueueDeclareOptions::default()
            },
            arguments,
        )
        .await
        .map_err(AmqpError::declare)?;
    Ok(())
}

/// Declares the `x-delayed-message` exchange (direct-typed) and binds `origin` to it under
/// `routing_key`, so a delayed copy the plugin releases returns to the origin queue.
#[cfg(feature = "plugin-dme")]
async fn declare_delayed_exchange(
    channel: &Channel,
    exchange: &str,
    origin: &str,
    routing_key: &str,
) -> Result<(), AmqpError> {
    let mut arguments = FieldTable::default();
    // The delayed exchange wraps an underlying routing type; direct routes by the exact key.
    arguments.insert(
        ShortString::from("x-delayed-type"),
        AMQPValue::LongString("direct".into()),
    );
    channel
        .exchange_declare(
            convert::short(exchange, "delayed exchange name")?,
            lapin::ExchangeKind::Custom("x-delayed-message".to_owned()),
            ExchangeDeclareOptions {
                durable: true,
                ..ExchangeDeclareOptions::default()
            },
            arguments,
        )
        .await
        .map_err(AmqpError::declare)?;
    channel
        .queue_bind(
            convert::short(origin, "queue name")?,
            convert::short(exchange, "delayed exchange name")?,
            convert::short(routing_key, "routing key")?,
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await
        .map_err(AmqpError::declare)?;
    Ok(())
}
