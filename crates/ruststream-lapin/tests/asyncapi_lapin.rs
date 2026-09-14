//! What the generated `AsyncAPI` document says about a service on `RabbitMQ`.
//!
//! The document is built before anything connects and is published afterwards, so every value
//! here comes from a descriptor or a policy and none of them is a credential. The `amqp` binding
//! is where the broker's own vocabulary lands: the queue's settings on the channel, the
//! acknowledgement on the receive operation, the message properties on a send, and the header a
//! client reads a reply address from.

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
