"""Shared Avro and Protobuf schema definitions for Iggy format examples.

The Avro schema mirrors ``schemas/event.avsc`` and the Protobuf message class
corresponds to ``schemas/event.proto``.  Both are defined here so that the
producer/consumer example scripts and their tests can import from a single
place without needing a running code-generation step.
"""
import io

import fastavro
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

# ---------------------------------------------------------------------------
# Avro
# ---------------------------------------------------------------------------

EVENT_AVRO_SCHEMA: dict = {
    "type": "record",
    "name": "Event",
    "namespace": "iggy.example",
    "fields": [
        {"name": "id",   "type": "int"},
        {"name": "text", "type": "string"},
        {"name": "ts",   "type": "string"},
    ],
}

_parsed_avro_schema = fastavro.parse_schema(EVENT_AVRO_SCHEMA)


def avro_serialize(record: dict) -> bytes:
    """Serialize *record* to Avro bytes using schemaless encoding."""
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, _parsed_avro_schema, record)
    return buf.getvalue()


def avro_deserialize(data: bytes) -> dict:
    """Deserialize schemaless Avro *data* back to a record ``dict``."""
    return fastavro.schemaless_reader(io.BytesIO(data), _parsed_avro_schema)


# ---------------------------------------------------------------------------
# Protobuf
# ---------------------------------------------------------------------------

def _build_event_class():
    """Build the ``Event`` protobuf message class dynamically.

    This avoids the need for a ``protoc`` code-generation step while still
    using the official ``google.protobuf`` runtime.  The descriptor mirrors
    ``schemas/event.proto``.
    """
    proto_file = descriptor_pb2.FileDescriptorProto()
    proto_file.name = "iggy_example_event.proto"
    proto_file.package = "iggy.example"
    proto_file.syntax = "proto3"

    msg_proto = proto_file.message_type.add()
    msg_proto.name = "Event"

    for fname, fnum, ftype in [
        ("id",   1, descriptor_pb2.FieldDescriptorProto.TYPE_INT32),
        ("text", 2, descriptor_pb2.FieldDescriptorProto.TYPE_STRING),
        ("ts",   3, descriptor_pb2.FieldDescriptorProto.TYPE_STRING),
    ]:
        field = msg_proto.field.add()
        field.name = fname
        field.number = fnum
        field.type = ftype
        field.label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL

    pool = descriptor_pool.DescriptorPool()
    pool.Add(proto_file)
    return message_factory.GetMessageClass(
        pool.FindMessageTypeByName("iggy.example.Event")
    )


#: Protobuf ``Event`` message class – mirrors ``schemas/event.proto``.
Event = _build_event_class()


def protobuf_serialize(id: int, text: str, ts: str) -> bytes:
    """Serialize an event to Protobuf wire-format bytes."""
    return Event(id=id, text=text, ts=ts).SerializeToString()


def protobuf_deserialize(data: bytes):
    """Deserialize Protobuf *data* and return an ``Event`` message object."""
    return Event.FromString(data)
