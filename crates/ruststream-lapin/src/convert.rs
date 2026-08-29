//! Mapping between core [`HeaderMap`] and AMQP properties plus the header table.
//!
//! Well-known header names ride in the matching native AMQP property so external consumers see
//! them where the protocol puts them; every other header lands in the `headers` field table as a
//! `LongString` (an arbitrary byte string, so binary values survive the round trip).
//!
//! The properties a [publish step](crate::publish_step) carries are written last, over whatever
//! the headers resolved to.

use std::time::Duration;

use bytes::Bytes;
use lapin::BasicProperties;
use lapin::types::{AMQPValue, FieldTable, ShortString};
use ruststream::HeaderMap;

use crate::error::AmqpError;
use crate::publish_step::MessageProperties;

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

/// Builds publish properties from `headers`, routing well-known names into native properties,
/// and applies the per-message properties the publish step carried.
pub(crate) fn properties_for_publish(
    headers: &HeaderMap,
    persistent: bool,
    step: &MessageProperties,
) -> Result<BasicProperties, AmqpError> {
    let mut properties = BasicProperties::default().with_delivery_mode(if persistent {
        PERSISTENT
    } else {
        TRANSIENT
    });

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

    if let Some(priority) = step.priority {
        properties = properties.with_priority(priority);
    }
    if let Some(expiration) = &step.expiration {
        properties = properties.with_expiration(expiration.clone());
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

    /// The publish of a call that took no step.
    fn no_step() -> MessageProperties {
        MessageProperties::default()
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

        let properties = properties_for_publish(&headers, true, &no_step()).expect("valid headers");
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

        let properties =
            properties_for_publish(&headers, false, &no_step()).expect("valid headers");
        let back = headers_from_properties(&properties);
        assert_eq!(back.get("x-blob"), Some([0u8, 159, 146, 150].as_slice()));
        assert_eq!(properties.delivery_mode(), &Some(TRANSIENT));
    }

    #[test]
    fn oversized_property_value_is_an_error_not_a_panic() {
        let mut headers = HeaderMap::new();
        headers.insert("correlation-id", vec![b'x'; 300]);

        let err = properties_for_publish(&headers, true, &no_step()).expect_err("over 255 bytes");
        assert!(matches!(err, AmqpError::InvalidOptions(_)));
    }

    #[test]
    fn step_properties_land_on_the_native_fields() {
        let step = MessageProperties {
            priority: Some(7),
            expiration: Some(expiration_millis(Duration::from_secs(30))),
        };

        let properties =
            properties_for_publish(&HeaderMap::new(), true, &step).expect("valid properties");
        assert_eq!(properties.priority(), &Some(7));
        assert_eq!(
            properties.expiration().as_ref().map(ShortString::as_str),
            Some("30000")
        );
    }

    #[test]
    fn priority_and_expiration_headers_stay_in_the_table() {
        // The quiet failure the publish steps exist for: written as headers, both names travel
        // in the header table, where RabbitMQ reads neither of them.
        let headers: HeaderMap = [
            ("priority", b"7".as_slice()),
            ("expiration", b"30000".as_slice()),
        ]
        .into_iter()
        .collect();

        let properties = properties_for_publish(&headers, true, &no_step()).expect("valid headers");
        assert_eq!(properties.priority(), &None);
        assert_eq!(properties.expiration(), &None);
        let table = properties.headers().as_ref().expect("header table");
        assert!(table.inner().contains_key("priority"));
        assert!(table.inner().contains_key("expiration"));
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
