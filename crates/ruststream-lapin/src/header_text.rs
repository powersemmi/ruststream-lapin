//! The bytes a header-table value of any AMQP type reaches a handler as.
//!
//! Core headers are byte strings, and an AMQP header table is typed. A byte-string value
//! (`LongString`, `ShortString`, `ByteArray`) arrives byte for byte. Every other value arrives as
//! text, rendered the same way every time:
//!
//! | AMQP type | Header value |
//! |-----------|--------------|
//! | boolean | `true` or `false` |
//! | any integer width | decimal, `-8`, `4000000000` |
//! | timestamp | decimal seconds since the Unix epoch, `1700000000` |
//! | float, double | the shortest decimal that reads back as the same number, `0.1`; `NaN`, `inf`, `-inf` |
//! | decimal | the value with `scale` digits after the point, `123.45`, `0.005` |
//! | void | the empty value |
//! | array, table | compact JSON, a table's fields in name order |
//!
//! Inside JSON a byte string is a JSON string when it is UTF-8 and an array of its byte values
//! when it is not, a number keeps its rendering above (a non-finite float becomes the string of
//! it, since JSON has no literal for it), and void is `null`. So RabbitMQ's own `x-death` record
//! reads as `[{"count":1,"exchange":"","queue":"orders","reason":"rejected",...}]`.
//!
//! The text says what the value is and not which AMQP type carried it: an integer `5` and a
//! string `"5"` both arrive as `5`.

use std::fmt::{Display, Write};
use std::iter::repeat_n;
use std::str::from_utf8;

use bytes::Bytes;
use lapin::types::{AMQPValue, DecimalValue, FieldArray, FieldTable};

/// The header value `value` reaches a handler as.
pub(crate) fn header_bytes(value: &AMQPValue) -> Bytes {
    match value {
        AMQPValue::LongString(text) => Bytes::copy_from_slice(text.as_bytes()),
        AMQPValue::ShortString(text) => Bytes::copy_from_slice(text.as_str().as_bytes()),
        AMQPValue::ByteArray(bytes) => Bytes::copy_from_slice(bytes.as_slice()),
        AMQPValue::Void => Bytes::new(),
        // Outside JSON a non-finite float is its plain name, not a quoted one.
        AMQPValue::Float(number) => Bytes::from(number.to_string()),
        AMQPValue::Double(number) => Bytes::from(number.to_string()),
        // Every other scalar reads the same inside JSON and out of it.
        other => {
            let mut text = String::new();
            write_json(&mut text, other);
            Bytes::from(text)
        }
    }
}

/// Writes `value` as a JSON value.
fn write_json(out: &mut String, value: &AMQPValue) {
    match value {
        AMQPValue::Boolean(flag) => write_display(out, flag),
        AMQPValue::ShortShortInt(number) => write_display(out, number),
        AMQPValue::ShortShortUInt(number) => write_display(out, number),
        AMQPValue::ShortInt(number) => write_display(out, number),
        AMQPValue::ShortUInt(number) => write_display(out, number),
        AMQPValue::LongInt(number) => write_display(out, number),
        AMQPValue::LongUInt(number) => write_display(out, number),
        AMQPValue::LongLongInt(number) => write_display(out, number),
        AMQPValue::Timestamp(seconds) => write_display(out, seconds),
        AMQPValue::DecimalValue(decimal) => write_decimal(out, *decimal),
        AMQPValue::Float(number) => write_json_float(out, f64::from(*number), number),
        AMQPValue::Double(number) => write_json_float(out, *number, number),
        AMQPValue::LongString(text) => write_json_bytes(out, text.as_bytes()),
        AMQPValue::ShortString(text) => write_json_string(out, text.as_str()),
        AMQPValue::ByteArray(bytes) => write_json_bytes(out, bytes.as_slice()),
        AMQPValue::Void => out.push_str("null"),
        AMQPValue::FieldArray(array) => write_array(out, array),
        AMQPValue::FieldTable(table) => write_table(out, table),
    }
}

/// Writes a float as a JSON number, or as the string of its name where JSON has no literal for it.
///
/// `shown` is the float at its own width, so an `f32` keeps its own shortest form (`0.1`, not the
/// digits of its widening).
fn write_json_float(out: &mut String, number: f64, shown: &impl Display) {
    if number.is_finite() {
        write_display(out, shown);
    } else {
        write_json_string(out, &shown.to_string());
    }
}

fn write_display(out: &mut String, value: &impl Display) {
    // Writing into a `String` cannot fail.
    let _ = write!(out, "{value}");
}

/// Writes `value` scaled down by ten to the `scale`: `12345` at scale 2 is `123.45`.
fn write_decimal(out: &mut String, decimal: DecimalValue) {
    let digits = decimal.value.to_string();
    let scale = usize::from(decimal.scale);
    if scale == 0 {
        out.push_str(&digits);
        return;
    }
    if digits.len() > scale {
        let (whole, fraction) = digits.split_at(digits.len() - scale);
        out.push_str(whole);
        out.push('.');
        out.push_str(fraction);
    } else {
        out.push_str("0.");
        out.extend(repeat_n('0', scale - digits.len()));
        out.push_str(&digits);
    }
}

fn write_array(out: &mut String, array: &FieldArray) {
    out.push('[');
    for (index, value) in array.as_slice().iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        write_json(out, value);
    }
    out.push(']');
}

fn write_table(out: &mut String, table: &FieldTable) {
    out.push('{');
    for (index, (name, value)) in table.inner().iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        write_json_string(out, name.as_str());
        out.push(':');
        write_json(out, value);
    }
    out.push('}');
}

/// Writes bytes as a JSON string when they are UTF-8, and as the array of their values when not.
fn write_json_bytes(out: &mut String, bytes: &[u8]) {
    if let Ok(text) = from_utf8(bytes) {
        write_json_string(out, text);
        return;
    }
    out.push('[');
    for (index, byte) in bytes.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        write_display(out, byte);
    }
    out.push(']');
}

fn write_json_string(out: &mut String, text: &str) {
    out.push('"');
    for character in text.chars() {
        match character {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            control if u32::from(control) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", u32::from(control));
            }
            other => out.push(other),
        }
    }
    out.push('"');
}
