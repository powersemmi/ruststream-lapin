//! Mapping between core [`Headers`] and AMQP properties plus the header table.
//!
//! Well-known header names ride in the matching native AMQP property so external consumers see
//! them where the protocol puts them; every other header lands in the `headers` field table as a
//! `LongString` (an arbitrary byte string, so binary values survive the round trip).

use bytes::Bytes;
use lapin::BasicProperties;
use lapin::types::{AMQPValue, FieldTable, ShortString};
use ruststream::Headers;

use crate::error::AmqpError;

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

/// Builds publish properties from `headers`, routing well-known names into native properties.
pub(crate) fn properties_for_publish(
    headers: &Headers,
    persistent: bool,
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

    Ok(properties)
}

/// Rebuilds core [`Headers`] from delivery properties.
///
/// Native properties come back under their well-known header names. Table values of a
/// non-byte-string type (numbers, nested tables such as `x-death`) are skipped: core headers are
/// byte-valued, and inventing a canonical encoding here would be lossy in a quieter way.
pub(crate) fn headers_from_properties(properties: &BasicProperties) -> Headers {
    let mut headers = Headers::new();

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

    #[test]
    fn round_trips_well_known_and_custom_headers() {
        let headers: Headers = [
            ("Content-Type", b"application/json".as_slice()),
            ("correlation-id", b"c-1"),
            ("reply-to", b"replies"),
            ("message-id", b"m-1"),
            ("x-tenant", b"acme"),
        ]
        .into_iter()
        .collect();

        let properties = properties_for_publish(&headers, true).expect("valid headers");
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
        let mut headers = Headers::new();
        headers.insert("x-blob", Bytes::from_static(&[0u8, 159, 146, 150]));

        let properties = properties_for_publish(&headers, false).expect("valid headers");
        let back = headers_from_properties(&properties);
        assert_eq!(back.get("x-blob"), Some([0u8, 159, 146, 150].as_slice()));
        assert_eq!(properties.delivery_mode(), &Some(TRANSIENT));
    }

    #[test]
    fn oversized_property_value_is_an_error_not_a_panic() {
        let mut headers = Headers::new();
        headers.insert("correlation-id", vec![b'x'; 300]);

        let err = properties_for_publish(&headers, true).expect_err("over 255 bytes");
        assert!(matches!(err, AmqpError::InvalidOptions(_)));
    }
}
