/**
 * Shared Avro and Protobuf schema helpers for the Iggy format examples.
 *
 * The Avro schema mirrors `src/schemas/event.avsc` and the Protobuf schema
 * mirrors `src/schemas/event.proto`.  Both are defined here so that the
 * producer/consumer example scripts and their tests can import from a single
 * place without requiring a code-generation step.
 */

import avsc from "avsc";
import protobuf from "protobufjs";

// ---------------------------------------------------------------------------
// Avro
// ---------------------------------------------------------------------------

export interface EventRecord {
  id: number;
  text: string;
  ts: string;
}

const EVENT_AVRO_SCHEMA = {
  type: "record" as const,
  name: "Event",
  namespace: "iggy.example",
  fields: [
    { name: "id",   type: "int"    },
    { name: "text", type: "string" },
    { name: "ts",   type: "string" },
  ],
};

const avroType = avsc.Type.forSchema(EVENT_AVRO_SCHEMA);

/** Serialize an {@link EventRecord} to Avro bytes using {@link avsc} `toBuffer`. */
export function avroSerialize(record: EventRecord): Buffer {
  return avroType.toBuffer(record);
}

/** Deserialize Avro bytes back to an {@link EventRecord}. */
export function avroDeserialize(data: Buffer): EventRecord {
  return avroType.fromBuffer(data) as EventRecord;
}

// ---------------------------------------------------------------------------
// Protobuf
// ---------------------------------------------------------------------------

const PROTO_SCHEMA = `
  syntax = "proto3";
  package iggy.example;
  message Event {
    int32  id   = 1;
    string text = 2;
    string ts   = 3;
  }
`;

const protoRoot = protobuf.parse(PROTO_SCHEMA).root;
const EventType = protoRoot.lookupType("iggy.example.Event");

/** Serialize an event to Protobuf wire-format bytes. */
export function protobufSerialize(id: number, text: string, ts: string): Uint8Array {
  const msg = EventType.create({ id, text, ts });
  return EventType.encode(msg).finish();
}

/** Deserialize Protobuf bytes and return a plain object with the event fields. */
export function protobufDeserialize(data: Uint8Array): { id: number; text: string; ts: string } {
  const decoded = EventType.decode(data);
  return EventType.toObject(decoded, { defaults: true }) as { id: number; text: string; ts: string };
}
