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

use std::time::Duration;

use bytes::Bytes;
use lapin::BasicProperties;
use lapin::types::{AMQPValue, FieldTable, ShortString};
use ruststream::HeaderMap;

use crate::error::AmqpError;
use crate::publish_step::{EXPIRATION_HEADER, LapinPublishOptions, PRIORITY_HEADER};

/// Delivery mode 2 marks a message persistent; 1 is transient.
const PERSISTENT: u8 = 2;
const TRANSIENT: u8 = 1;

/// Header names that map onto native AMQP properties instead of the header table.
const PROPERTY_HEADERS: [&str; 4] = ["content-type", "correlation-id", "reply-to", "message-id"];

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
        .and_then(|value| std::str::from_utf8(value).ok())
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
/// Native properties come back under their well-known header names. Table values of a
/// non-byte-string type (numbers, nested tables such as `x-death`) are skipped: core headers are
/// byte-valued, and inventing a canonical encoding here would be lossy in a quieter way.
pub(crate) fn headers_from_properties(properties: &BasicProperties) -> HeaderMap {
    let mut headers = HeaderMap::new();

    if let Some(value) = properties.content_type() {
        headers.insert(
            "content-type",
            Bytes::copy_from_slice(value.as_str().as_bytes()),
        );
    }
    if let Some(value) = properties.correlation_id() {
        headers.insert(
            "correlation-id",
            Bytes::copy_from_slice(value.as_str().as_bytes()),
        );
    }
    if let Some(value) = properties.reply_to() {
        headers.insert(
            "reply-to",
            Bytes::copy_from_slice(value.as_str().as_bytes()),
        );
    }
    if let Some(value) = properties.message_id() {
        headers.insert(
            "message-id",
            Bytes::copy_from_slice(value.as_str().as_bytes()),
        );
    }
    if let Some(value) = properties.priority() {
        headers.insert(PRIORITY_HEADER, Bytes::from(value.to_string()));
    }
    if let Some(value) = properties.expiration() {
        headers.insert(
            EXPIRATION_HEADER,
            Bytes::copy_from_slice(value.as_str().as_bytes()),
        );
    }

    if let Some(table) = properties.headers() {
        for (name, value) in table.inner() {
            let bytes = match value {
                AMQPValue::LongString(v) => Bytes::copy_from_slice(v.as_bytes()),
                AMQPValue::ShortString(v) => Bytes::copy_from_slice(v.as_str().as_bytes()),
                AMQPValue::ByteArray(v) => Bytes::copy_from_slice(v.as_slice()),
                _ => continue,
            };
            headers.insert(name.as_str(), bytes);
        }
    }

    headers
}

#[cfg(test)]
mod tests {
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
