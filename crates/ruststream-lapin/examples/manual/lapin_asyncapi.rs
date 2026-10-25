//! The service of `examples/lapin_asyncapi.rs` without the macros: the handlers are named types
//! mounted on their queue descriptors, and `main` is written by hand. The tests read the same
//! document.
//!
//! ```text
//! cargo run --example manual_lapin_asyncapi --features asyncapi -- asyncapi gen
//! cargo test --example manual_lapin_asyncapi --features asyncapi
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::any::type_name;
use std::env;
use std::future::ready;
use std::process::ExitCode;
use std::time::Duration;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use ruststream_lapin::prelude::*;

#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Ask {
    sku: String,
}

#[derive(Debug, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Stock {
    available: bool,
}

// `#[derive(Outgoing)]` with no `name`, by hand: the answer goes wherever the request asked.
impl OutgoingDestination for Stock {
    type Form = CallerName;
}

impl MessageHeaders for Stock {
    type Contract = NoHeaders;
}

#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Event {
    sku: String,
    in_stock: u64,
}

/// A request to refill a sold-out item, sent to the queue of the same name.
#[derive(Debug, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Restock {
    sku: String,
}

// `#[outgoing(name = "inventory.restock")]` by hand.
impl OutgoingDestination for Restock {
    type Form = FixedName;
    const DESTINATION: &'static str = "inventory.restock";
}

impl MessageHeaders for Restock {
    type Contract = NoHeaders;
}

/// The slot restock requests leave through.
struct Restocks;

impl OutSlot for Restocks {
    const NAME: &'static str = "Restocks";
    // `Restock` fixes its destination, so no transform on this slot may name one.
    type Destination = Reads;

    // What `#[publishes(Restock)]` declares: the document reports the slot's channel and send.
    fn outgoing() -> Vec<OutgoingMessageMetadata> {
        vec![OutgoingMessageMetadata::new(
            Restock::DESTINATION,
            type_name::<Restock>(),
        )]
    }
}

impl PublishedThrough<Restocks> for Restock {}

struct Check;

impl Handle<Ask, Stock> for Check {
    fn handle(
        &self,
        ask: &Ask,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Stock, HandlerOutcome>> {
        ready(Ok(Stock {
            available: !ask.sku.is_empty(),
        }))
    }
}

struct OnEvent;

impl<Entry> Handle<Event, (), Outs<(Entry,)>> for OnEvent
where
    Entry: OutEntry<Restocks, Wire: Publisher>,
{
    async fn handle(
        &self,
        event: &Event,
        outs: &Outs<(Entry,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        if event.in_stock > 0 {
            return Ok(());
        }
        let restock = Restock {
            sku: event.sku.clone(),
        };
        if outs
            .get(Restocks)
            .message(&restock)
            .publish()
            .await
            .is_err()
        {
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
}

// --8<-- [start:extension]
/// What a headers exchange matches a binding on: every entry, or any one of them.
fn gold_tier() -> FieldTable {
    let mut arguments = FieldTable::default();
    arguments.insert("x-match".into(), AMQPValue::LongString("all".into()));
    arguments.insert("tier".into(), AMQPValue::LongString("gold".into()));
    arguments
}

/// The events queue, fed by a topic exchange and by a headers exchange.
fn events() -> RabbitQueue {
    RabbitQueue::new("inventory.events")
        .bind(RabbitExchange::topic("events"), "inventory.*")
        .bind_with(RabbitExchange::headers("attributes"), "", gold_tier())
}
// --8<-- [end:extension]

// --8<-- [start:channels]
/// A queue that lives as long as this service's connection.
fn checks() -> RabbitQueue {
    RabbitQueue::new("inventory.check")
        .durable(false)
        .exclusive(true)
        .auto_delete(true)
}

// --8<-- [start:app]
// --8<-- [start:operations]
fn service(broker: LapinBroker) -> RustStream {
    let inventory = AppInfo::new("inventory", "1.0.0");
    RustStream::new(inventory).with_broker_labeled("rabbit", broker, |b| {
        let check = subscriber(checks(), Check).reply().to("inventory.unrouted");
        b.include(check.build())
            .dead_letter("inventory.dead")
            .out_reply(Publish::default().exchange("answers"))
            .transform(DirectReplyTo);
        b.include(subscriber(events(), OnEvent).build())
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

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

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
            serde_json::from_str(include_str!("../../tests/snapshots/asyncapi-amqp.json"))?;
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
        // The queue moves a dead letter itself, so its channel is an address and no publish of
        // the service's.
        assert_eq!(channels["inventory.dead"]["address"], "inventory.dead");
        assert!(
            channels["inventory.dead"]["bindings"].is_null(),
            "{channels}"
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
        // The server moves the dead letter with the properties it arrived with.
        assert_eq!(
            operations["send_inventory_check_inventory_dead"]["action"],
            "send"
        );
        assert!(
            operations["send_inventory_check_inventory_dead"]["bindings"].is_null(),
            "{operations}"
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
