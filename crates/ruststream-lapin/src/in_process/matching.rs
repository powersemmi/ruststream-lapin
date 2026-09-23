//! How an exchange picks the queues a message goes to, the server's rules reproduced: a topic
//! pattern against a routing key, and a headers binding against a message's header table.

use lapin::types::{AMQPValue, FieldTable, ShortString};

/// Whether the topic `pattern` of a binding matches `routing_key`.
///
/// Both are dot-separated words. `*` stands for exactly one word and `#` for zero or more, so
/// `order.*` matches `order.created` and not `order.created.eu`, and `order.#` matches both and
/// `order` itself.
pub(crate) fn topic_matches(pattern: &str, routing_key: &str) -> bool {
    let pattern: Vec<&str> = pattern.split('.').collect();
    let key: Vec<&str> = routing_key.split('.').collect();
    words_match(&pattern, &key)
}

fn words_match(pattern: &[&str], key: &[&str]) -> bool {
    match pattern.split_first() {
        None => key.is_empty(),
        Some((&"#", rest)) => (0..=key.len()).any(|skipped| words_match(rest, &key[skipped..])),
        Some((&word, rest)) => key.split_first().is_some_and(|(&first, key_rest)| {
            (word == "*" || word == first) && words_match(rest, key_rest)
        }),
    }
}

/// Whether a headers binding's `arguments` match a message's header `table`.
///
/// `x-match` decides between every entry (`all`, the default) and any one of them (`any`); the
/// `all-with-x` and `any-with-x` forms count the `x-` entries too, which the plain forms skip. An
/// entry whose value is `void` asks only for the header to be there. A value matches when it is the
/// same value: the header table a publish of this crate carries holds byte strings, so a binding
/// that names a number matches none of its headers, on the server as here.
pub(crate) fn headers_match(arguments: &FieldTable, table: Option<&FieldTable>) -> bool {
    let mode = match arguments.inner().get(&ShortString::from("x-match")) {
        Some(AMQPValue::LongString(value)) => value.to_string(),
        Some(AMQPValue::ShortString(value)) => value.to_string(),
        _ => "all".to_owned(),
    };
    let any = mode.starts_with("any");
    let with_x = mode.ends_with("-with-x");
    let mut entries = arguments
        .inner()
        .iter()
        .filter(|(name, _)| name.as_str() != "x-match")
        .filter(|(name, _)| with_x || !name.as_str().starts_with("x-"))
        .peekable();
    if entries.peek().is_none() {
        // Nothing to match on: `all` of nothing holds for every message, `any` of nothing for none.
        return !any;
    }
    let matches = |(name, expected): (&ShortString, &AMQPValue)| {
        let present = table.and_then(|table| table.inner().get(name));
        match (expected, present) {
            (_, None) => false,
            (AMQPValue::Void, Some(_)) => true,
            (expected, Some(present)) => same_value(expected, present),
        }
    };
    if any {
        entries.any(matches)
    } else {
        entries.all(matches)
    }
}

/// Whether two table values are the same value, a text written as a long or a short string
/// being the same text.
fn same_value(expected: &AMQPValue, present: &AMQPValue) -> bool {
    match (text_of(expected), text_of(present)) {
        (Some(expected), Some(present)) => expected == present,
        _ => expected == present,
    }
}

fn text_of(value: &AMQPValue) -> Option<&[u8]> {
    match value {
        AMQPValue::LongString(value) => Some(value.as_bytes()),
        AMQPValue::ShortString(value) => Some(value.as_str().as_bytes()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use lapin::types::{AMQPValue, FieldTable, ShortString};

    use super::{headers_match, topic_matches};

    #[test]
    fn a_star_is_one_word_and_a_hash_is_any_number() {
        assert!(topic_matches("order.*", "order.created"));
        assert!(!topic_matches("order.*", "order.created.eu"));
        assert!(!topic_matches("order.*", "order"));
        assert!(topic_matches("order.#", "order"));
        assert!(topic_matches("order.#", "order.created.eu"));
        assert!(topic_matches("#", "anything.at.all"));
        assert!(topic_matches("*.created", "order.created"));
        assert!(!topic_matches("*.created", "order.updated"));
        assert!(topic_matches("order.#.eu", "order.eu"));
        assert!(topic_matches("order.#.eu", "order.created.eu"));
        assert!(topic_matches("order.created", "order.created"));
        assert!(!topic_matches("order.created", "order.updated"));
    }

    fn table(entries: &[(&str, AMQPValue)]) -> FieldTable {
        let mut table = FieldTable::default();
        for (name, value) in entries {
            table.insert(ShortString::from(*name), value.clone());
        }
        table
    }

    fn text(value: &str) -> AMQPValue {
        AMQPValue::LongString(value.into())
    }

    #[test]
    fn all_needs_every_entry_and_any_needs_one() {
        let message = table(&[("region", text("eu")), ("tier", text("gold"))]);
        let all = table(&[("region", text("eu")), ("tier", text("silver"))]);
        let any = table(&[
            ("x-match", text("any")),
            ("region", text("eu")),
            ("tier", text("silver")),
        ]);

        assert!(!headers_match(&all, Some(&message)));
        assert!(headers_match(&any, Some(&message)));
    }

    #[test]
    fn a_binding_with_no_entries_takes_every_message_under_all_and_none_under_any() {
        let message = table(&[("region", text("eu"))]);

        assert!(headers_match(&FieldTable::default(), Some(&message)));
        assert!(headers_match(&FieldTable::default(), None));
        assert!(!headers_match(
            &table(&[("x-match", text("any"))]),
            Some(&message)
        ));
    }

    #[test]
    fn a_number_never_matches_a_text_header() {
        let message = table(&[("tier", text("3"))]);
        let binding = table(&[("tier", AMQPValue::LongLongInt(3))]);

        assert!(!headers_match(&binding, Some(&message)));
    }
}
