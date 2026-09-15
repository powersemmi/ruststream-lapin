//! What the generated `AsyncAPI` document says about a service on `RabbitMQ`.
//!
//! The document is built before anything connects and is published afterwards, so every value
//! here comes from a descriptor or a policy and none of them is a credential. The `amqp` binding
//! is where the broker's own vocabulary lands: the queue's settings on the channel, the
//! acknowledgement on the receive operation, the message properties on a send, and the header a
//! client reads a reply address from. What binds a queue to its exchanges has no field there, so
//! it rides the crate's own extension beside it.

#![cfg(all(feature = "asyncapi", feature = "testing"))]

use std::time::Duration;

use ruststream::DescribeServer;
use ruststream::asyncapi::build_spec;
use ruststream::conformance::harness;
use ruststream_lapin::prelude::*;
use ruststream_lapin::testing::LapinTestBroker;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Deserialize)]
struct Ask {
    sku: String,
}

/// The answer goes wherever the request asked, so the type declares no destination of its own.
#[derive(Debug, Outgoing, Serialize)]
struct Stock {
    available: bool,
}

// The queue carries settings that are not the defaults, so an assertion on the document cannot
// pass on a value the crate never read.
#[subscriber(
    RabbitQueue::new("inventory.check").durable(false).exclusive(true).auto_delete(true),
    publish("inventory.unrouted")
)]
async fn check(ask: &Ask) -> Stock {
    Stock {
        available: !ask.sku.is_empty(),
    }
}

#[derive(Debug, Deserialize)]
struct Event {
    id: u64,
}

/// What a headers exchange matches a binding on: every entry, or any one of them.
fn gold_in_eu() -> FieldTable {
    let mut arguments = FieldTable::default();
    arguments.insert("x-match".into(), AMQPValue::LongString("all".into()));
    arguments.insert("tier".into(), AMQPValue::LongString("gold".into()));
    arguments
}

// A queue fed by two exchanges: one that routes on the key, one that routes on the binding's
// arguments. The order is what the document has to keep.
#[subscriber(
    RabbitQueue::new("inventory.events")
        .bind(RabbitExchange::topic("events"), "inventory.*")
        .bind_with(RabbitExchange::headers("attributes"), "", gold_in_eu())
)]
async fn on_event(event: &Event) -> HandlerOutcome {
    let _ = event.id;
    HandlerOutcome::ack()
}

/// The document of a service that answers over direct reply-to and carries a spent delivery away.
fn document() -> Value {
    let app = RustStream::new(AppInfo::new("inventory", "1.0.0"))
        .server(
            "rabbit",
            LapinBroker::new("amqp://rabbit:5672").describe_server(),
        )
        .with_broker(LapinTestBroker::new(), |b| {
            b.include(check)
                .max_attempts(nonzero!(3u32))
                .dead_letter("inventory.dead")
                .out_reply(
                    Publish::default()
                        .exchange("answers")
                        .priority(4)
                        .expiration(Duration::from_secs(30)),
                )
                .transform(DirectReplyTo);
            b.include(on_event);
        });
    let json = build_spec(&app)
        .to_json()
        .expect("the document must serialize");
    serde_json::from_str(&json).expect("valid JSON")
}

/// Asserts that every leaf of `excerpt` stands at the same path in `document`.
fn carries(document: &Value, excerpt: &Value, path: &str) {
    match excerpt {
        Value::Object(fields) => {
            for (key, value) in fields {
                carries(&document[key], value, &format!("{path}/{key}"));
            }
        }
        leaf => assert_eq!(
            document, leaf,
            "the document differs from the excerpt at {path}"
        ),
    }
}

// The excerpt the documentation shows is this document, so a page cannot promise a field the
// crate stopped writing.
#[test]
fn the_documented_excerpt_is_what_the_crate_emits() {
    let excerpt: Value = serde_json::from_str(include_str!("snapshots/asyncapi-amqp.json"))
        .expect("the excerpt is valid JSON");
    carries(&document(), &excerpt, "");
}

#[test]
fn the_server_names_the_wire_version_behind_the_protocol() {
    let value = document();
    let server = &value["servers"]["rabbit"];

    assert_eq!(server["protocol"], "amqp");
    // AMQP 1.0 is a different protocol with a binding key of its own, and the host says nothing
    // about which one a client has to speak.
    assert_eq!(server["protocolVersion"], "0.9.1");
    assert_eq!(server["host"], "rabbit:5672");
}

#[test]
fn a_subscription_reports_its_queue_and_its_acknowledgement() {
    let value = document();
    let channel = &value["channels"]["inventory.check"]["bindings"]["amqp"];

    assert_eq!(channel["is"], "queue");
    assert_eq!(channel["queue"]["name"], "inventory.check");
    assert_eq!(channel["queue"]["durable"], false);
    assert_eq!(channel["queue"]["exclusive"], true);
    assert_eq!(channel["queue"]["autoDelete"], true);
    assert_eq!(channel["bindingVersion"], "0.3.0");

    let operation = &value["operations"]["receive_inventory_check"]["bindings"]["amqp"];
    assert_eq!(operation["ack"], true);
    assert_eq!(operation["bindingVersion"], "0.3.0");
}

// The specification's binding describes a channel as a queue or as a routing key and has no field
// for what binds the two, so the routing table a service expects rides the crate's extension.
#[test]
fn a_subscription_reports_the_bindings_it_declares() {
    let value = document();
    let extension = &value["channels"]["inventory.events"]["bindings"]["x-ruststream-amqp"];
    let bindings = &extension["bindings"];

    assert_eq!(
        bindings.as_array().expect("a list of bindings").len(),
        2,
        "one entry per binding the descriptor declared"
    );
    assert_eq!(bindings[0]["exchange"], "events");
    assert_eq!(bindings[0]["type"], "topic");
    assert_eq!(bindings[0]["routingKey"], "inventory.*");
    assert!(
        bindings[0]["arguments"].is_null(),
        "an exchange that routes on the key matches on nothing else"
    );

    assert_eq!(bindings[1]["exchange"], "attributes");
    assert_eq!(bindings[1]["type"], "headers");
    // An empty key is a binding key like any other, so it is written rather than left out.
    assert_eq!(bindings[1]["routingKey"], "");
    assert_eq!(bindings[1]["arguments"]["x-match"], "all");
    assert_eq!(bindings[1]["arguments"]["tier"], "gold");

    // An extension is not a binding, so the field that belongs to one is absent.
    assert!(extension["bindingVersion"].is_null());
}

#[test]
fn a_subscription_with_no_binding_reports_no_extension() {
    let value = document();

    assert!(
        value["channels"]["inventory.check"]["bindings"]["x-ruststream-amqp"].is_null(),
        "a queue nothing is bound to has no routing table to report"
    );
}

#[test]
fn a_reply_reports_the_exchange_it_leaves_through() {
    let value = document();
    let channel = &value["channels"]["inventory.unrouted"]["bindings"]["amqp"];

    assert_eq!(channel["is"], "routingKey");
    assert_eq!(channel["exchange"]["name"], "answers");
    // A named exchange leaves the routing key as the channel's address, so the binding has no
    // second place to write the destination into.
    assert!(channel["queue"].is_null());
}

#[test]
fn a_dead_letter_reports_the_properties_its_copies_carry() {
    let value = document();
    let channel = &value["channels"]["inventory.dead"]["bindings"]["amqp"];

    // The dead-letter copy leaves through the broker's default publish policy, which publishes on
    // the default exchange: there the routing key is the queue it lands in. The policy holds no
    // destination, so the name can only be the one the mount site declared.
    assert_eq!(channel["is"], "queue");
    assert_eq!(channel["queue"]["name"], "inventory.dead");

    let operation = &value["operations"]["send_inventory_check_inventory_dead"]["bindings"]["amqp"];
    assert_eq!(operation["deliveryMode"], 2);
    assert!(operation["priority"].is_null());
    assert!(operation["expiration"].is_null());
}

#[test]
fn a_reply_routed_by_header_says_where_a_client_reads_the_address() {
    let value = document();

    // The transform decides the destination per delivery, so the channel has no address and the
    // operation names the header the address is read from.
    assert!(value["channels"]["inventory.unrouted"]["address"].is_null());
    assert_eq!(
        value["operations"]["receive_inventory_check"]["reply"]["address"]["location"],
        "$message.header#/reply-to",
    );
}

#[test]
fn neither_the_server_nor_a_binding_carries_the_password() {
    harness::describes_without_credentials(
        &LapinBroker::new("amqp://svc:hunter2@rabbit:5672/prod"),
        &RabbitQueue::new("inventory.check"),
        "hunter2",
    );
}
