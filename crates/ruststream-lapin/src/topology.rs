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
use crate::queue::{QueueType, RabbitQueue};

/// Declares the exchanges, queue, bindings, and delay backend `def` describes.
pub(crate) async fn declare(
    channel: &Channel,
    def: &RabbitQueue,
    broker_default: Option<QueueType>,
) -> Result<(), AmqpError> {
    for (exchange, _) in def.bindings() {
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

    let queue_type = def.queue_type_or(broker_default);
    if queue_type == Some(QueueType::Quorum) && !def.is_durable() {
        return Err(AmqpError::InvalidOptions(format!(
            "queue {:?} is a quorum queue and must stay durable; drop `.durable(false)` or pick \
             `QueueType::Classic`",
            def.name(),
        )));
    }

    let mut arguments = def.declare_arguments().clone();
    if let Some(queue_type) = queue_type {
        arguments.insert(
            ShortString::from("x-queue-type"),
            AMQPValue::LongString(queue_type.as_str().into()),
        );
    }
    channel
        .queue_declare(
            convert::short(def.name(), "queue name")?,
            QueueDeclareOptions {
                durable: def.is_durable(),
                exclusive: def.is_exclusive(),
                auto_delete: def.is_auto_delete(),
                ..QueueDeclareOptions::default()
            },
            arguments,
        )
        .await
        .map_err(AmqpError::declare)?;

    for (exchange, routing_key) in def.bindings() {
        channel
            .queue_bind(
                convert::short(def.name(), "queue name")?,
                convert::short(exchange.name(), "exchange name")?,
                convert::short(routing_key, "routing key")?,
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .map_err(AmqpError::declare)?;
    }

    if let Some(delay) = def.delay_config() {
        declare_delay_backend(channel, delay, def.name()).await?;
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
        ShortString::from("x-dead-letter-exchange"),
        AMQPValue::LongString(String::new().into()),
    );
    arguments.insert(
        ShortString::from("x-dead-letter-routing-key"),
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
