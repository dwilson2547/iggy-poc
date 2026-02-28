//! Shared Avro and Protobuf schema helpers for the Iggy format examples.
//!
//! The Avro schema mirrors `schemas/event.avsc` and the Protobuf struct
//! mirrors `schemas/event.proto`.  Both are defined here so that the
//! producer/consumer example binaries and their `#[cfg(test)]` tests can
//! import from a single place without requiring a code-generation step.

use apache_avro::{from_avro_datum, to_avro_datum, Schema};
use apache_avro::types::Value;

// ---------------------------------------------------------------------------
// Avro
// ---------------------------------------------------------------------------

/// Raw Avro schema JSON string mirroring `schemas/event.avsc`.
pub const EVENT_AVRO_SCHEMA_JSON: &str = r#"
{
  "type":      "record",
  "name":      "Event",
  "namespace": "iggy.example",
  "fields": [
    {"name": "id",   "type": "int"},
    {"name": "text", "type": "string"},
    {"name": "ts",   "type": "string"}
  ]
}
"#;

/// Returns the parsed [`Schema`] for the `Event` Avro record.
pub fn avro_schema() -> Schema {
    Schema::parse_str(EVENT_AVRO_SCHEMA_JSON).expect("EVENT_AVRO_SCHEMA_JSON is always valid")
}

/// Serialize an event to schemaless Avro bytes.
pub fn avro_serialize(id: i32, text: &str, ts: &str) -> Result<Vec<u8>, apache_avro::Error> {
    let schema = avro_schema();
    let value = Value::Record(vec![
        ("id".to_string(),   Value::Int(id)),
        ("text".to_string(), Value::String(text.to_string())),
        ("ts".to_string(),   Value::String(ts.to_string())),
    ]);
    to_avro_datum(&schema, value)
}

/// Deserialize schemaless Avro bytes back to a `(id, text, ts)` tuple.
pub fn avro_deserialize(data: &[u8]) -> Result<(i32, String, String), apache_avro::Error> {
    let schema = avro_schema();
    let mut cursor = std::io::Cursor::new(data);
    let value = from_avro_datum(&schema, &mut cursor, None)?;
    match value {
        Value::Record(fields) => {
            let mut id: i32 = 0;
            let mut text = String::new();
            let mut ts = String::new();
            for (name, val) in fields {
                match (name.as_str(), val) {
                    ("id",   Value::Int(v))    => id   = v,
                    ("text", Value::String(v)) => text = v,
                    ("ts",   Value::String(v)) => ts   = v,
                    _ => {}
                }
            }
            Ok((id, text, ts))
        }
        other => Err(apache_avro::Error::new(apache_avro::error::Details::DeserializeValue(
            format!("expected Record, got {:?}", other),
        ))),
    }
}

// ---------------------------------------------------------------------------
// Protobuf
// ---------------------------------------------------------------------------

/// `Event` Protobuf message struct mirroring `schemas/event.proto`.
///
/// The `prost::Message` derive macro generates efficient encode/decode
/// implementations without requiring a `protoc` code-generation step.
#[derive(Clone, PartialEq, prost::Message)]
pub struct Event {
    /// Monotonically incrementing message ID.
    #[prost(int32, tag = "1")]
    pub id: i32,
    /// Human-readable message body.
    #[prost(string, tag = "2")]
    pub text: String,
    /// ISO-8601 UTC timestamp of the event.
    #[prost(string, tag = "3")]
    pub ts: String,
}

/// Serialize an event to Protobuf wire-format bytes.
pub fn protobuf_serialize(id: i32, text: &str, ts: &str) -> Vec<u8> {
    use prost::Message;
    Event { id, text: text.to_string(), ts: ts.to_string() }.encode_to_vec()
}

/// Deserialize Protobuf bytes back to an [`Event`].
pub fn protobuf_deserialize(data: &[u8]) -> Result<Event, prost::DecodeError> {
    use prost::Message;
    Event::decode(data)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // Avro tests -----------------------------------------------------------

    #[test]
    fn avro_roundtrip_produces_original_fields() {
        let data = avro_serialize(1, "hello from Rust Avro producer", "2024-01-01T00:00:00Z")
            .expect("serialize");
        let (id, text, ts) = avro_deserialize(&data).expect("deserialize");
        assert_eq!(id, 1);
        assert_eq!(text, "hello from Rust Avro producer");
        assert_eq!(ts, "2024-01-01T00:00:00Z");
    }

    #[test]
    fn avro_serialized_bytes_are_not_empty() {
        let data = avro_serialize(42, "test", "2024-01-01T00:00:00Z").expect("serialize");
        assert!(!data.is_empty());
    }

    #[test]
    fn avro_different_records_produce_different_bytes() {
        let a = avro_serialize(1, "first",  "2024-01-01T00:00:00Z").expect("serialize");
        let b = avro_serialize(2, "second", "2024-01-01T00:00:00Z").expect("serialize");
        assert_ne!(a, b);
    }

    #[test]
    fn avro_successive_records_carry_incrementing_ids() {
        for i in 1..=3 {
            let data = avro_serialize(i, "msg", "2024-01-01T00:00:00Z").expect("serialize");
            let (id, _, _) = avro_deserialize(&data).expect("deserialize");
            assert_eq!(id, i);
        }
    }

    // Protobuf tests -------------------------------------------------------

    #[test]
    fn protobuf_roundtrip_restores_all_fields() {
        let data = protobuf_serialize(1, "hello from Rust Protobuf producer", "2024-01-01T00:00:00Z");
        let event = protobuf_deserialize(&data).expect("deserialize");
        assert_eq!(event.id, 1);
        assert_eq!(event.text, "hello from Rust Protobuf producer");
        assert_eq!(event.ts, "2024-01-01T00:00:00Z");
    }

    #[test]
    fn protobuf_serialized_bytes_are_not_empty() {
        let data = protobuf_serialize(42, "test", "2024-01-01T00:00:00Z");
        assert!(!data.is_empty());
    }

    #[test]
    fn protobuf_different_ids_produce_different_bytes() {
        let a = protobuf_serialize(1, "same text", "2024-01-01T00:00:00Z");
        let b = protobuf_serialize(2, "same text", "2024-01-01T00:00:00Z");
        assert_ne!(a, b);
    }

    #[test]
    fn protobuf_successive_events_carry_incrementing_ids() {
        for i in 1..=3i32 {
            let data = protobuf_serialize(i, "msg", "2024-01-01T00:00:00Z");
            let event = protobuf_deserialize(&data).expect("deserialize");
            assert_eq!(event.id, i);
        }
    }

    #[test]
    fn protobuf_default_values_roundtrip() {
        let data = protobuf_serialize(0, "", "");
        let event = protobuf_deserialize(&data).expect("deserialize");
        assert_eq!(event.id, 0);
        assert_eq!(event.text, "");
        assert_eq!(event.ts, "");
    }
}
