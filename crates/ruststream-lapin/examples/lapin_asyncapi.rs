//! The `AsyncAPI` document of a `RabbitMQ` service: the `amqp` bindings say which server and AMQP
//! version a client speaks, which queue stands behind each channel, how a receive settles and which
//! properties a send carries. The bindings a queue declares ride the `x-ruststream-amqp`
//! extension. The tests read the document the app generates.
//!
//! ```text
//! cargo run --example lapin_asyncapi --features asyncapi -- asyncapi gen
//! cargo test --example lapin_asyncapi --features asyncapi
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::env;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use ruststream_lapin::prelude::*;

#[derive(Debug, Deserialize)]
struct Ask {
    sku: String,
}

/// The answer goes wherever the request asked, so the type declares no destination of its own.
#[derive(Debug, Serialize, Outgoing)]
struct Stock {
    available: bool,
}

#[derive(Debug, Deserialize)]
struct Event {
    sku: String,
    in_stock: u64,
}

/// A request to refill a sold-out item, sent to the queue of the same name.
#[derive(Debug, Serialize, Outgoing)]
#[outgoing(name = "inventory.restock")]
struct Restock {
    sku: String,
}

#[derive(OutSlot)]
#[publishes(Restock)]
struct Restocks;

// --8<-- [start:extension]
/// What a headers exchange matches a binding on: every entry, or any one of them.
fn gold_tier() -> FieldTable {
    let mut arguments = FieldTable::default();
    arguments.insert("x-match".into(), AMQPValue::LongString("all".into()));
    arguments.insert("tier".into(), AMQPValue::LongString("gold".into()));
    arguments
}

#[subscriber(
    RabbitQueue::new("inventory.events")
        .bind(RabbitExchange::topic("events"), "inventory.*")
        .bind_with(RabbitExchange::headers("attributes"), "", gold_tier())
)]
async fn on_event(event: &Event, Out(restocks): Out<impl Publisher, Restocks>) -> HandlerOutcome {
    if event.in_stock > 0 {
        return HandlerOutcome::ack();
    }
    let restock = Restock {
        sku: event.sku.clone(),
    };
    if restocks.message(&restock).publish().await.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}
// --8<-- [end:extension]

// --8<-- [start:channels]
#[subscriber(
    RabbitQueue::new("inventory.check").durable(false).exclusive(true).auto_delete(true),
    reply("inventory.unrouted")
)]
async fn check(ask: &Ask) -> Stock {
    Stock {
        available: !ask.sku.is_empty(),
    }
}

// --8<-- [start:app]
// --8<-- [start:operations]
fn service(broker: LapinBroker) -> RustStream {
    let inventory = AppInfo::new("inventory", "1.0.0");
    RustStream::new(inventory).with_broker_labeled("rabbit", broker, |b| {
        b.include(check)
            .max_attempts(nonzero!(3u32))
            .dead_letter("inventory.dead")
            .out_reply(Publish::default().exchange("answers"))
            .transform(DirectReplyTo);
        b.include(on_event)
            .out(
                Restocks,
                Publish::default()
                    .priority(4)
                    .expiration(Duration::from_secs(30)),
            )
            .build();
    })
}
// --8<-- [end:operations]
// --8<-- [end:channels]

#[ruststream::app]
fn app() -> RustStream {
    let password = env::var("AMQP_PASSWORD").unwrap_or_default();
    service(broker(&password))
}
// --8<-- [end:app]

// --8<-- [start:server]
/// The production server: the URI carries the service's credentials and its virtual host.
fn broker(password: &str) -> LapinBroker {
    LapinBroker::new(format!("amqp://inventory:{password}@rabbit:5672/prod"))
}
// --8<-- [end:server]

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::hash::{BuildHasher, RandomState};

    use ruststream::asyncapi::build_spec;
    use serde_json::{Value, json};

    use super::{broker, service};

    /// A password no document could hold by coincidence, drawn afresh for every run.
    fn password() -> String {
        format!("{:016x}", RandomState::new().hash_one("password"))
    }

    fn document_with(password: &str) -> Result<Value, Box<dyn Error>> {
        Ok(serde_json::from_str(
            &build_spec(&service(broker(password))).to_json()?,
        )?)
    }

    fn document() -> Result<Value, Box<dyn Error>> {
        document_with(&password())
    }

    /// Asserts that every leaf of `excerpt` stands at the same path in `document`.
    fn carries(document: &Value, excerpt: &Value, path: &str) {
        match excerpt {
            Value::Object(fields) => {
                for (key, value) in fields {
                    carries(&document[key], value, &format!("{path}/{key}"));
                }
            }
            leaf => assert_eq!(document, leaf, "the document differs at {path}"),
        }
    }

    // The excerpt the Extension page shows is this service's document.
    #[test]
    fn the_shown_excerpt_is_what_the_service_emits() -> Result<(), Box<dyn Error>> {
        let excerpt: Value =
            serde_json::from_str(include_str!("../tests/snapshots/asyncapi-amqp.json"))?;
        carries(&document()?, &excerpt, "");
        Ok(())
    }

    #[test]
    fn the_server_is_the_host_and_the_wire_version() -> Result<(), Box<dyn Error>> {
        let spec = document()?;

        assert_eq!(
            spec["servers"]["rabbit"],
            json!({
                "host": "rabbit:5672",
                "protocol": "amqp",
                "protocolVersion": "0.9.1",
            })
        );
        Ok(())
    }

    #[test]
    fn the_document_carries_no_credential_and_no_virtual_host() -> Result<(), Box<dyn Error>> {
        let password = password();
        let text = document_with(&password)?.to_string();

        assert!(!text.contains(&password), "{text}");
        assert!(!text.contains("/prod"), "{text}");
        Ok(())
    }

    #[test]
    fn a_subscription_channel_is_its_queue() -> Result<(), Box<dyn Error>> {
        let spec = document()?;

        assert_eq!(
            spec["channels"]["inventory.check"]["bindings"]["amqp"],
            json!({
                "is": "queue",
                "queue": {
                    "name": "inventory.check",
                    "durable": false,
                    "exclusive": true,
                    "autoDelete": true,
                },
                "bindingVersion": "0.3.0",
            })
        );
        Ok(())
    }

    #[test]
    fn a_publish_channel_is_a_routing_key_or_a_queue() -> Result<(), Box<dyn Error>> {
        let spec = document()?;
        let channels = &spec["channels"];

        // The reply leaves through the named exchange `answers`.
        assert_eq!(
            channels["inventory.unrouted"]["bindings"]["amqp"],
            json!({ "is": "routingKey", "exchange": { "name": "answers" }, "bindingVersion": "0.3.0" })
        );
        // The dead letter leaves through the default exchange, where the key is a queue's name.
        assert_eq!(
            channels["inventory.dead"]["bindings"]["amqp"],
            json!({ "is": "queue", "queue": { "name": "inventory.dead" }, "bindingVersion": "0.3.0" })
        );
        Ok(())
    }

    #[test]
    fn operations_report_the_acknowledgement_and_the_properties() -> Result<(), Box<dyn Error>> {
        let spec = document()?;
        let operations = &spec["operations"];

        assert_eq!(
            operations["receive_inventory_check"]["bindings"]["amqp"],
            json!({ "ack": true, "bindingVersion": "0.3.0" })
        );
        assert_eq!(
            operations["send_inventory_events_inventory_restock"]["bindings"]["amqp"],
            json!({
                "deliveryMode": 2,
                "priority": 4,
                "expiration": 30_000,
                "bindingVersion": "0.3.0",
            })
        );
        assert_eq!(
            operations["send_inventory_check_inventory_dead"]["bindings"]["amqp"],
            json!({ "deliveryMode": 2, "bindingVersion": "0.3.0" })
        );
        // `DirectReplyTo` answers at the address each request carries.
        assert_eq!(
            operations["receive_inventory_check"]["reply"]["address"]["location"],
            "$message.header#/reply-to"
        );
        Ok(())
    }

    #[test]
    fn the_extension_lists_the_bindings_in_declaration_order() -> Result<(), Box<dyn Error>> {
        let spec = document()?;
        let channels = &spec["channels"];

        assert_eq!(
            channels["inventory.events"]["bindings"]["x-ruststream-amqp"],
            json!({
                "bindings": [
                    { "exchange": "events", "type": "topic", "routingKey": "inventory.*" },
                    {
                        "exchange": "attributes",
                        "type": "headers",
                        "routingKey": "",
                        "arguments": { "x-match": "all", "tier": "gold" },
                    },
                ],
            })
        );
        // A queue nothing is bound to has no routing table to report.
        assert!(
            channels["inventory.check"]["bindings"]["x-ruststream-amqp"].is_null(),
            "{channels}"
        );
        Ok(())
    }
}
