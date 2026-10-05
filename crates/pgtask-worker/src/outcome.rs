//! Keeps handler outcomes inside what `pgtask.tasks` accepts, so one bad value cannot fail a write.
//!
//! The limits mirror `tasks_result_size_check` and `tasks_error_size_check`, which measure
//! `octet_length(value::text)`: the size of the value as PostgreSQL prints `jsonb`, not as
//! `serde_json` prints it. `jsonb` also rejects `\u0000`, which `serde_json` accepts.

use serde_json::{Map, Value, json};

/// `tasks_result_size_check`: `octet_length(result::text) <= 1048576`.
pub(crate) const RESULT_LIMIT_BYTES: usize = 1_048_576;
/// `tasks_error_size_check` and `attempts_error_size_check`: `octet_length(error::text) <= 262144`.
pub(crate) const ERROR_LIMIT_BYTES: usize = 262_144;

/// What `jsonb` cannot store in place of a NUL character.
const NUL_REPLACEMENT: &str = "\\u0000";
/// Marks the end of a string that was shortened to fit.
const ELLIPSIS: &str = "…";

/// A handler result ready to write, or the terminal error that replaces it.
#[derive(Debug, PartialEq)]
pub(crate) enum PreparedResult {
    Result(Value),
    TooLarge(Value),
}

/// Escapes NUL characters and replaces a result that would exceed the column limit.
pub(crate) fn prepare_result(mut result: Value) -> PreparedResult {
    escape_nul(&mut result);
    let bytes = jsonb_text_len(&result);
    if bytes <= RESULT_LIMIT_BYTES {
        PreparedResult::Result(result)
    } else {
        PreparedResult::TooLarge(json!({
            "type": "result_too_large",
            "bytes": bytes,
            "limit": RESULT_LIMIT_BYTES,
        }))
    }
}

/// Escapes NUL characters and shortens an error until it fits, marking it `truncated`.
pub(crate) fn prepare_error(mut error: Value) -> Value {
    escape_nul(&mut error);
    let bytes = jsonb_text_len(&error);
    if bytes <= ERROR_LIMIT_BYTES {
        return error;
    }
    truncate_error(&error, bytes)
}

/// The terminal error recorded when PostgreSQL rejects a value the checks above let through.
pub(crate) fn rejected_value_error(kind: &str, message: &str) -> Value {
    let mut message = message.replace('\0', NUL_REPLACEMENT);
    truncate_string(&mut message, 4_096);
    json!({"type": kind, "message": message})
}

fn escape_nul(value: &mut Value) {
    match value {
        Value::String(text) => {
            if text.contains('\0') {
                *text = text.replace('\0', NUL_REPLACEMENT);
            }
        }
        Value::Array(items) => items.iter_mut().for_each(escape_nul),
        Value::Object(map) => {
            if map.keys().any(|key| key.contains('\0')) {
                *map = std::mem::take(map)
                    .into_iter()
                    .map(|(key, value)| (key.replace('\0', NUL_REPLACEMENT), value))
                    .collect();
            }
            map.values_mut().for_each(escape_nul);
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

/// Shortens every string longer than the largest cap that still fits; falls back to a summary.
fn truncate_error(error: &Value, bytes: usize) -> Value {
    let marked = |value: Value| -> Value {
        let mut map = match value {
            Value::Object(map) => map,
            other => Map::from_iter([("value".to_owned(), other)]),
        };
        map.insert("truncated".to_owned(), Value::Bool(true));
        map.insert("original_bytes".to_owned(), json!(bytes));
        Value::Object(map)
    };
    let fits = |cap: usize| {
        let candidate = marked(cap_strings(error.clone(), cap));
        (jsonb_text_len(&candidate) <= ERROR_LIMIT_BYTES).then_some(candidate)
    };
    let mut low = 0;
    let mut high = longest_string(error);
    let mut best = fits(low);
    while best.is_some() && low < high {
        let middle = low + (high - low).div_ceil(2);
        match fits(middle) {
            Some(candidate) => {
                low = middle;
                best = Some(candidate);
            }
            None => high = middle - 1,
        }
    }
    best.unwrap_or_else(|| {
        let kind = error
            .get("type")
            .and_then(Value::as_str)
            .filter(|kind| kind.len() <= 255)
            .unwrap_or("handler_error");
        let mut summary = json!({"type": kind, "truncated": true, "original_bytes": bytes});
        if let Some(retryable) = error.get("retryable").filter(|value| value.is_boolean()) {
            summary["retryable"] = retryable.clone();
        }
        summary
    })
}

fn cap_strings(value: Value, cap: usize) -> Value {
    match value {
        Value::String(mut text) => {
            truncate_string(&mut text, cap);
            Value::String(text)
        }
        Value::Array(items) => Value::Array(items.into_iter().map(|item| cap_strings(item, cap)).collect()),
        Value::Object(map) => Value::Object(
            map.into_iter()
                .map(|(key, item)| (key, cap_strings(item, cap)))
                .collect(),
        ),
        other => other,
    }
}

/// Keeps at most `cap` bytes of `text`, cut at a character boundary, and marks the cut.
fn truncate_string(text: &mut String, cap: usize) {
    if text.len() <= cap {
        return;
    }
    let mut end = cap;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text.truncate(end);
    text.push_str(ELLIPSIS);
}

fn longest_string(value: &Value) -> usize {
    match value {
        Value::String(text) => text.len(),
        Value::Array(items) => items.iter().map(longest_string).max().unwrap_or(0),
        Value::Object(map) => map.values().map(longest_string).max().unwrap_or(0),
        Value::Null | Value::Bool(_) | Value::Number(_) => 0,
    }
}

/// The length of `value::text` for a `jsonb` value: `", "` and `": "` separators, PostgreSQL
/// string escaping, and numbers printed by `numeric_out` without an exponent.
pub(crate) fn jsonb_text_len(value: &Value) -> usize {
    match value {
        Value::Null | Value::Bool(true) => 4,
        Value::Bool(false) => 5,
        Value::Number(number) => numeric_text_len(&number.to_string()),
        Value::String(text) => jsonb_string_len(text),
        Value::Array(items) => 2 + items.iter().map(jsonb_text_len).sum::<usize>() + 2 * items.len().saturating_sub(1),
        Value::Object(map) => {
            2 + map
                .iter()
                .map(|(key, item)| jsonb_string_len(key) + 2 + jsonb_text_len(item))
                .sum::<usize>()
                + 2 * map.len().saturating_sub(1)
        }
    }
}

fn jsonb_string_len(text: &str) -> usize {
    2 + text
        .chars()
        .map(|character| match character {
            '"' | '\\' | '\u{08}' | '\u{0C}' | '\n' | '\r' | '\t' => 2,
            control if u32::from(control) < 0x20 => 6,
            other => other.len_utf8(),
        })
        .sum::<usize>()
}

/// `numeric_out` never prints an exponent: `1e21` prints 22 digits and `1.5e-7` prints `0.00000015`.
fn numeric_text_len(rendered: &str) -> usize {
    let Some((mantissa, exponent)) = rendered.split_once(['e', 'E']) else {
        return rendered.len();
    };
    let Ok(exponent) = exponent.parse::<i64>() else {
        return rendered.len();
    };
    let sign = usize::from(mantissa.starts_with('-'));
    let mantissa = mantissa.trim_start_matches(['-', '+']);
    let (integer, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let integer_len = i64::try_from(integer.len()).unwrap_or(i64::MAX);
    let fraction_len = i64::try_from(fraction.len()).unwrap_or(i64::MAX);
    let scale = fraction_len.saturating_sub(exponent).max(0);
    let integer_digits = integer_len.saturating_add(exponent).max(1);
    let digits = integer_digits.saturating_add(if scale > 0 { scale + 1 } else { 0 });
    sign + usize::try_from(digits).unwrap_or(usize::MAX)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    /// Expected sizes were checked against `octet_length(value::jsonb::text)` on PostgreSQL 17.
    #[test]
    fn measures_values_as_postgres_prints_jsonb() {
        assert_eq!(
            jsonb_text_len(&json!({"a": 1, "b": [true, null]})),
            r#"{"a": 1, "b": [true, null]}"#.len()
        );
        assert_eq!(jsonb_text_len(&json!("tab\tquote\"é")), r#""tab\tquote\"é""#.len());
        assert_eq!(jsonb_text_len(&json!("\u{01}")), r#""\u0001""#.len());
        assert_eq!(jsonb_text_len(&json!([])), 2);
        assert_eq!(numeric_text_len("1e21"), 22);
        assert_eq!(numeric_text_len("1.5e-7"), "0.00000015".len());
        assert_eq!(numeric_text_len("-2.5e3"), "-2500".len());
        assert_eq!(numeric_text_len("12.25"), 5);
    }

    #[test]
    fn escapes_nul_in_strings_and_keys() {
        let PreparedResult::Result(result) = prepare_result(json!({"a\u{0}": ["x\u{0}y"]})) else {
            panic!("a small result fits");
        };
        assert_eq!(result, json!({"a\\u0000": ["x\\u0000y"]}));
        assert_eq!(prepare_error(json!("\u{0}")), json!("\\u0000"));
    }

    #[test]
    fn replaces_an_oversized_result_with_a_terminal_error() {
        let result = json!("x".repeat(RESULT_LIMIT_BYTES));
        assert_eq!(
            prepare_result(result),
            PreparedResult::TooLarge(json!({
                "type": "result_too_large",
                "bytes": RESULT_LIMIT_BYTES + 2,
                "limit": RESULT_LIMIT_BYTES,
            }))
        );
        let fits = json!("x".repeat(RESULT_LIMIT_BYTES - 2));
        assert_eq!(prepare_result(fits.clone()), PreparedResult::Result(fits));
    }

    #[test]
    fn truncates_an_oversized_error_and_keeps_its_shape() {
        let error = json!({"type": "handler_error", "message": "e".repeat(300_000), "code": 7});
        let prepared = prepare_error(error);
        assert!(jsonb_text_len(&prepared) <= ERROR_LIMIT_BYTES);
        assert!(jsonb_text_len(&prepared) > ERROR_LIMIT_BYTES - 64);
        assert_eq!(prepared["type"], "handler_error");
        assert_eq!(prepared["code"], 7);
        assert_eq!(prepared["truncated"], true);
        assert_eq!(prepared["original_bytes"], 300_051);
        assert!(prepared["message"].as_str().unwrap().ends_with(ELLIPSIS));
    }

    #[test]
    fn summarizes_an_error_that_strings_cannot_shrink() {
        let error = json!({"type": "handler_error", "items": vec![1; 100_000]});
        assert_eq!(
            prepare_error(error),
            json!({"type": "handler_error", "truncated": true, "original_bytes": 300_036})
        );
        let error = json!({"type": "handler_error", "items": vec![1; 100_000], "retryable": false});
        assert_eq!(
            prepare_error(error),
            json!({"type": "handler_error", "truncated": true, "original_bytes": 300_056, "retryable": false})
        );
        let prepared = prepare_error(json!("e".repeat(300_000)));
        assert_eq!(prepared["truncated"], true);
        assert!(prepared["value"].as_str().unwrap().len() < ERROR_LIMIT_BYTES);
    }

    #[test]
    fn small_errors_pass_through_unchanged() {
        let error = json!({"type": "handler_error", "message": "boom"});
        assert_eq!(prepare_error(error.clone()), error);
    }
}
