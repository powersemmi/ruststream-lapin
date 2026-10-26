//! Mapping between core [`HeaderMap`] and AMQP properties plus the header table.
//!
//! The message's identity headers ride in the matching native AMQP property so external consumers
//! see them where the protocol puts them; every other header lands in the `headers` field table as
//! a `LongString` (an arbitrary byte string, so binary values survive the round trip).
//!
//! The delivery properties a publish does not take from headers - the priority, the expiration,
//! the delivery mode - come from [`LapinPublishOptions`] instead, resolved from the policy and the
//! call site. A delivery still reports them back as headers, under [`PRIORITY_HEADER`] and
//! [`EXPIRATION_HEADER`], so a handler reads them where it reads everything else.

use std::str::from_utf8;
use std::time::Duration;

use bytes::Bytes;
use lapin::BasicProperties;
use lapin::types::{AMQPValue, FieldTable, ShortString};
use ruststream::{HeaderMap, Str};

use crate::error::AmqpError;
use crate::header_text;
use crate::publish_step::{EXPIRATION_HEADER, LapinPublishOptions, PRIORITY_HEADER};

/// Delivery mode 2 marks a message persistent; 1 is transient.
const PERSISTENT: u8 = 2;
const TRANSIENT: u8 = 1;

/// Header names that map onto native AMQP properties instead of the header table.
const PROPERTY_HEADERS: [&str; 4] = ["content-type", "correlation-id", "reply-to", "message-id"];

/// Reads a header-table value that carries a count, whatever integer width the server chose.
///
/// `RabbitMQ` picks the narrowest type that fits, so a counter the crate reads back has no single
/// wire type to match on; a value that is not an integer at all is not a count and answers `None`.
pub(crate) fn counter(value: &AMQPValue) -> Option<u64> {
    match value {
        AMQPValue::ShortShortInt(value) => u64::try_from(*value).ok(),
        AMQPValue::ShortShortUInt(value) => Some(u64::from(*value)),
        AMQPValue::ShortInt(value) => u64::try_from(*value).ok(),
        AMQPValue::ShortUInt(value) => Some(u64::from(*value)),
        AMQPValue::LongInt(value) => u64::try_from(*value).ok(),
        AMQPValue::LongUInt(value) => Some(u64::from(*value)),
        AMQPValue::LongLongInt(value) => u64::try_from(*value).ok(),
        _ => None,
    }
}

pub(crate) fn short(value: &str, what: &str) -> Result<ShortString, AmqpError> {
    ShortString::try_new(value).map_err(|err| {
        AmqpError::InvalidOptions(format!(
            "{what} {value:?} is not a valid short string: {err}"
        ))
    })
}

/// The settings a redelivered copy of `headers` carries: the priority the original delivery
/// reported, and nothing else.
///
/// The one place a header decides a property, and it is the inverse mapping rather than the
/// publish path: a delayed redelivery re-sends a message whose priority was already on the wire,
/// and [`headers_from_properties`] is what put it in this map. Dropping it would quietly demote
/// every delayed copy on a priority queue.
pub(crate) fn redelivery_options(headers: &HeaderMap) -> LapinPublishOptions {
    let priority = headers
        .get(PRIORITY_HEADER)
        .and_then(|value| from_utf8(value).ok())
        .and_then(|text| text.parse::<u8>().ok());
    LapinPublishOptions {
        priority,
        ..LapinPublishOptions::PERSISTENT
    }
}

/// Renders `ttl` as the decimal milliseconds AMQP carries in the `expiration` property.
pub(crate) fn expiration_millis(ttl: Duration) -> ShortString {
    let millis = u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX);
    // Zero means "drop unless a consumer is already waiting", so a non-zero TTL never rounds to it.
    let millis = if millis == 0 && !ttl.is_zero() {
        1
    } else {
        millis
    };
    ShortString::from(millis.to_string())
}

/// Builds publish properties from `headers` and the resolved per-message `options`.
///
/// `options` is the call site's settings over the policy's, already resolved by the publisher.
pub(crate) fn properties_for_publish(
    headers: &HeaderMap,
    options: &LapinPublishOptions,
) -> Result<BasicProperties, AmqpError> {
    let mut properties =
        BasicProperties::default().with_delivery_mode(if options.persistent.unwrap_or(true) {
            PERSISTENT
        } else {
            TRANSIENT
        });

    if let Some(priority) = options.priority {
        properties = properties.with_priority(priority);
    }
    if let Some(ttl) = options.expiration {
        properties = properties.with_expiration(expiration_millis(ttl));
    }

    if let Some(value) = headers.content_type() {
        properties = properties.with_content_type(short(value, "content-type header")?);
    }
    if let Some(value) = headers.correlation_id() {
        properties = properties.with_correlation_id(short(value, "correlation-id header")?);
    }
    if let Some(value) = headers.reply_to() {
        properties = properties.with_reply_to(short(value, "reply-to header")?);
    }
    if let Some(value) = headers.message_id() {
        properties = properties.with_message_id(short(value, "message-id header")?);
    }

    let mut table = FieldTable::default();
    for (name, value) in headers.iter() {
        if PROPERTY_HEADERS.contains(&name) {
            continue;
        }
        table.insert(
            short(name, "header name")?,
            AMQPValue::LongString(value.into()),
        );
    }
    if !table.inner().is_empty() {
        properties = properties.with_headers(table);
    }

    Ok(properties)
}

/// Rebuilds core [`HeaderMap`] from delivery properties.
///
/// Native properties come back under their well-known header names. Every header-table value
/// arrives: a byte string byte for byte, any other type as the text
/// [`header_text`](crate::header_text) documents (a number in decimal, an array or a table such as
/// `x-death` as JSON).
pub(crate) fn headers_from_properties(properties: &BasicProperties) -> HeaderMap {
    let mut headers = HeaderMap::new();

    if let Some(value) = properties.content_type() {
        headers.insert(
            Str::from_static("content-type"),
            Bytes::copy_from_slice(value.as_str().as_bytes()),
        );
    }
    if let Some(value) = properties.correlation_id() {
        headers.insert(
            Str::from_static("correlation-id"),
            Bytes::copy_from_slice(value.as_str().as_bytes()),
        );
    }
    if let Some(value) = properties.reply_to() {
        headers.insert(
            Str::from_static("reply-to"),
            Bytes::copy_from_slice(value.as_str().as_bytes()),
        );
    }
    if let Some(value) = properties.message_id() {
        headers.insert(
            Str::from_static("message-id"),
            Bytes::copy_from_slice(value.as_str().as_bytes()),
        );
    }
    if let Some(value) = properties.priority() {
        headers.insert(
            Str::from_static(PRIORITY_HEADER),
            Bytes::from(value.to_string()),
        );
    }
    if let Some(value) = properties.expiration() {
        headers.insert(
            Str::from_static(EXPIRATION_HEADER),
            Bytes::copy_from_slice(value.as_str().as_bytes()),
        );
    }

    if let Some(table) = properties.headers() {
        for (name, value) in table.inner() {
            headers.insert(name.as_str(), header_text::header_bytes(value));
        }
    }

    headers
}

#[cfg(test)]
mod tests {
    use lapin::types::{ByteArray, DecimalValue, FieldArray};

    use super::*;

    /// The per-message settings a publish carries when nothing adjusted them: the publish
    /// policies' own defaults.
    fn persistent_defaults() -> LapinPublishOptions {
        LapinPublishOptions {
            persistent: Some(true),
            ..LapinPublishOptions::default()
        }
    }

    #[test]
    fn round_trips_well_known_and_custom_headers() {
        let headers: HeaderMap = [
            ("Content-Type", b"application/json".as_slice()),
            ("correlation-id", b"c-1"),
            ("reply-to", b"replies"),
            ("message-id", b"m-1"),
            ("x-tenant", b"acme"),
        ]
        .into_iter()
        .collect();

        let properties =
            properties_for_publish(&headers, &persistent_defaults()).expect("valid headers");
        assert_eq!(
            properties.content_type().as_ref().map(ShortString::as_str),
            Some("application/json")
        );
        assert_eq!(properties.delivery_mode(), &Some(PERSISTENT));

        let back = headers_from_properties(&properties);
        assert_eq!(back.content_type(), Some("application/json"));
        assert_eq!(back.correlation_id(), Some("c-1"));
        assert_eq!(back.reply_to(), Some("replies"));
        assert_eq!(back.message_id(), Some("m-1"));
        assert_eq!(back.get("x-tenant"), Some(b"acme".as_slice()));
    }

    #[test]
    fn binary_header_values_survive() {
        let mut headers = HeaderMap::new();
        headers.insert("x-blob", Bytes::from_static(&[0u8, 159, 146, 150]));

        let properties = properties_for_publish(
            &headers,
            &LapinPublishOptions {
                persistent: Some(false),
                ..LapinPublishOptions::default()
            },
        )
        .expect("valid headers");
        let back = headers_from_properties(&properties);
        assert_eq!(back.get("x-blob"), Some([0u8, 159, 146, 150].as_slice()));
        assert_eq!(properties.delivery_mode(), &Some(TRANSIENT));
    }

    #[test]
    fn oversized_property_value_is_an_error_not_a_panic() {
        let mut headers = HeaderMap::new();
        headers.insert("correlation-id", vec![b'x'; 300]);

        let err =
            properties_for_publish(&headers, &persistent_defaults()).expect_err("over 255 bytes");
        assert!(matches!(err, AmqpError::InvalidOptions(_)));
    }

    #[test]
    fn the_resolved_settings_land_on_the_native_fields_and_come_back_as_headers() {
        let options = LapinPublishOptions {
            priority: Some(7),
            expiration: Some(Duration::from_secs(30)),
            persistent: Some(true),
        };

        let properties =
            properties_for_publish(&HeaderMap::new(), &options).expect("valid properties");
        assert_eq!(properties.priority(), &Some(7));
        assert_eq!(
            properties.expiration().as_ref().map(ShortString::as_str),
            Some("30000")
        );
        // They are frame fields, so nothing of them travels in the header table.
        assert!(properties.headers().is_none());

        let back = headers_from_properties(&properties);
        assert_eq!(back.get(PRIORITY_HEADER), Some(b"7".as_slice()));
        assert_eq!(back.get(EXPIRATION_HEADER), Some(b"30000".as_slice()));
    }

    #[test]
    fn a_header_never_sets_a_delivery_property() {
        // The quiet failure the publish steps exist for: under the protocol's own field names, or
        // under the names a delivery reports, both values travel in the header table, where
        // RabbitMQ reads none of them. The property comes from the options and from nowhere else.
        let headers: HeaderMap = [
            ("priority", b"7".as_slice()),
            ("expiration", b"30000".as_slice()),
            (PRIORITY_HEADER, b"7".as_slice()),
            (EXPIRATION_HEADER, b"30000".as_slice()),
        ]
        .into_iter()
        .collect();

        let properties =
            properties_for_publish(&headers, &persistent_defaults()).expect("valid headers");
        assert_eq!(properties.priority(), &None);
        assert_eq!(properties.expiration(), &None);
        let table = properties.headers().as_ref().expect("header table");
        assert!(table.inner().contains_key("priority"));
        assert!(table.inner().contains_key(PRIORITY_HEADER));
    }

    /// A delivery whose header table holds `fields`, as a foreign producer or the server wrote it.
    fn table_of(fields: Vec<(&str, AMQPValue)>) -> BasicProperties {
        let mut table = FieldTable::default();
        for (name, value) in fields {
            table.insert(ShortString::from(name), value);
        }
        BasicProperties::default().with_headers(table)
    }

    // A Java or Python client writes numbers, flags and timestamps with their own AMQP types, and
    // every one of them reaches the handler as its documented text.
    #[test]
    fn typed_scalars_from_a_foreign_producer_arrive_as_text() {
        let properties = table_of(vec![
            ("x-flag", AMQPValue::Boolean(true)),
            ("x-off", AMQPValue::Boolean(false)),
            ("x-i8", AMQPValue::ShortShortInt(-8)),
            ("x-u8", AMQPValue::ShortShortUInt(200)),
            ("x-i16", AMQPValue::ShortInt(-300)),
            ("x-u16", AMQPValue::ShortUInt(60_000)),
            ("x-i32", AMQPValue::LongInt(-70_000)),
            ("x-u32", AMQPValue::LongUInt(4_000_000_000)),
            ("x-i64", AMQPValue::LongLongInt(i64::MIN)),
            ("x-f32", AMQPValue::Float(1.5)),
            ("x-f64", AMQPValue::Double(0.1)),
            ("x-nan", AMQPValue::Double(f64::NAN)),
            (
                "x-price",
                AMQPValue::DecimalValue(DecimalValue {
                    scale: 2,
                    value: 12_345,
                }),
            ),
            (
                "x-tiny",
                AMQPValue::DecimalValue(DecimalValue { scale: 3, value: 5 }),
            ),
            ("x-at", AMQPValue::Timestamp(1_700_000_000)),
            ("x-none", AMQPValue::Void),
        ]);

        let headers = headers_from_properties(&properties);

        for (name, text) in [
            ("x-flag", "true"),
            ("x-off", "false"),
            ("x-i8", "-8"),
            ("x-u8", "200"),
            ("x-i16", "-300"),
            ("x-u16", "60000"),
            ("x-i32", "-70000"),
            ("x-u32", "4000000000"),
            ("x-i64", "-9223372036854775808"),
            ("x-f32", "1.5"),
            ("x-f64", "0.1"),
            ("x-nan", "NaN"),
            ("x-price", "123.45"),
            ("x-tiny", "0.005"),
            ("x-at", "1700000000"),
            ("x-none", ""),
        ] {
            assert_eq!(headers.get_str(name), Some(text), "header {name}");
        }
    }

    // The table RabbitMQ stamps on a dead-lettered message stays readable: an array of tables
    // becomes JSON, each table's fields in name order, strings and numbers in their JSON form.
    #[test]
    fn a_dead_letter_record_arrives_as_json() {
        let mut death = FieldTable::default();
        death.insert(ShortString::from("count"), AMQPValue::LongLongInt(1));
        death.insert(
            ShortString::from("exchange"),
            AMQPValue::LongString("".into()),
        );
        death.insert(
            ShortString::from("queue"),
            AMQPValue::LongString("orders".into()),
        );
        death.insert(
            ShortString::from("reason"),
            AMQPValue::LongString("rejected".into()),
        );
        death.insert(
            ShortString::from("routing-keys"),
            AMQPValue::FieldArray(FieldArray::from(vec![AMQPValue::LongString(
                "orders".into(),
            )])),
        );
        death.insert(
            ShortString::from("time"),
            AMQPValue::Timestamp(1_700_000_000),
        );
        let properties = table_of(vec![
            (
                "x-death",
                AMQPValue::FieldArray(FieldArray::from(vec![AMQPValue::FieldTable(death)])),
            ),
            (
                "x-first-death-queue",
                AMQPValue::LongString("orders".into()),
            ),
        ]);

        let headers = headers_from_properties(&properties);

        assert_eq!(
            headers.get_str("x-death"),
            Some(
                r#"[{"count":1,"exchange":"","queue":"orders","reason":"rejected","routing-keys":["orders"],"time":1700000000}]"#
            )
        );
        assert_eq!(headers.get_str("x-first-death-queue"), Some("orders"));
    }

    // Inside a table every value keeps a JSON form: flags, nothing, a number JSON has no literal
    // for, text that needs escaping, and bytes that are not text at all.
    #[test]
    fn nested_values_render_as_json_of_their_kind() {
        let mut nested = FieldTable::default();
        nested.insert(ShortString::from("flag"), AMQPValue::Boolean(false));
        nested.insert(ShortString::from("none"), AMQPValue::Void);
        nested.insert(ShortString::from("inf"), AMQPValue::Float(f32::INFINITY));
        nested.insert(ShortString::from("ratio"), AMQPValue::Float(0.1));
        nested.insert(
            ShortString::from("price"),
            AMQPValue::DecimalValue(DecimalValue {
                scale: 1,
                value: 15,
            }),
        );
        nested.insert(
            ShortString::from("quote"),
            AMQPValue::LongString("say \"hi\"\n\u{1}".into()),
        );
        nested.insert(
            ShortString::from("raw"),
            AMQPValue::ByteArray(ByteArray::from(vec![0xff, 0x00])),
        );
        let properties = table_of(vec![("x-meta", AMQPValue::FieldTable(nested))]);

        let headers = headers_from_properties(&properties);

        assert_eq!(
            headers.get_str("x-meta"),
            Some(
                r#"{"flag":false,"inf":"inf","none":null,"price":1.5,"quote":"say \"hi\"\n\u0001","ratio":0.1,"raw":[255,0]}"#
            )
        );
    }

    // The crate's own headers travel as byte strings and come back byte for byte, whichever of the
    // three byte-string types the producer chose.
    #[test]
    fn byte_string_values_come_back_unchanged() {
        let properties = table_of(vec![
            (
                "x-long",
                AMQPValue::LongString(vec![0u8, 159, 146, 150].into()),
            ),
            (
                "x-short",
                AMQPValue::ShortString(ShortString::from("plain")),
            ),
            (
                "x-bytes",
                AMQPValue::ByteArray(ByteArray::from(vec![0xff, 0x7b])),
            ),
        ]);

        let headers = headers_from_properties(&properties);

        assert_eq!(headers.get("x-long"), Some([0u8, 159, 146, 150].as_slice()));
        assert_eq!(headers.get("x-short"), Some(b"plain".as_slice()));
        assert_eq!(headers.get("x-bytes"), Some([0xff, 0x7b].as_slice()));
    }

    #[test]
    fn expiration_renders_whole_milliseconds() {
        assert_eq!(
            expiration_millis(Duration::from_millis(1500)).as_str(),
            "1500"
        );
        assert_eq!(expiration_millis(Duration::from_secs(2)).as_str(), "2000");
        assert_eq!(expiration_millis(Duration::ZERO).as_str(), "0");
        // A non-zero TTL never rounds down into "drop unless a consumer is already waiting".
        assert_eq!(expiration_millis(Duration::from_micros(500)).as_str(), "1");
    }
}
