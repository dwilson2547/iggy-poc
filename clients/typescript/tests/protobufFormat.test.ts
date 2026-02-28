import { describe, it, expect } from "vitest";
import { protobufSerialize, protobufDeserialize } from "../src/schemas.js";

describe("Protobuf serialization (schemas.ts)", () => {
  it("roundtrip restores all field values", () => {
    const bytes = protobufSerialize(1, "hello from protobuf producer", "2024-01-01T00:00:00.000Z");
    const event = protobufDeserialize(bytes);

    expect(event.id).toBe(1);
    expect(event.text).toBe("hello from protobuf producer");
    expect(event.ts).toBe("2024-01-01T00:00:00.000Z");
  });

  it("serialized output is a non-empty Uint8Array", () => {
    const bytes = protobufSerialize(42, "test", "2024-01-01T00:00:00.000Z");
    expect(bytes).toBeInstanceOf(Uint8Array);
    expect(bytes.length).toBeGreaterThan(0);
  });

  it("different ids produce different byte sequences", () => {
    const a = protobufSerialize(1, "same text", "2024-01-01T00:00:00.000Z");
    const b = protobufSerialize(2, "same text", "2024-01-01T00:00:00.000Z");
    expect(Buffer.from(a).equals(Buffer.from(b))).toBe(false);
  });

  it("successive events carry incrementing ids after roundtrip", () => {
    const ts = new Date().toISOString();
    const ids = [1, 2, 3].map((i) =>
      protobufDeserialize(protobufSerialize(i, "msg", ts)).id
    );
    expect(ids).toEqual([1, 2, 3]);
  });

  it("default (zero/empty) values survive a roundtrip", () => {
    const bytes = protobufSerialize(0, "", "");
    const event = protobufDeserialize(bytes);
    expect(event.id).toBe(0);
    expect(event.text).toBe("");
    expect(event.ts).toBe("");
  });
});

describe("Protobuf consumer message handling", () => {
  // Mirrors protobufConsumer.handleMessage without importing the file that calls main().
  function handleMessage(message: { payload: Buffer; offset: number | bigint }): string {
    const event = protobufDeserialize(message.payload);
    return `[offset=${message.offset}] id=${event.id} text='${event.text}' ts='${event.ts}'`;
  }

  it("decodes Protobuf bytes and includes event fields in output", () => {
    const payload = Buffer.from(protobufSerialize(7, "proto-test", "2024-06-01T12:00:00.000Z"));

    const output = handleMessage({ payload, offset: 99 });

    expect(output).toContain("proto-test");
    expect(output).toContain("offset=99");
    expect(output).toContain("id=7");
  });

  it("handles messages at arbitrary offsets", () => {
    const payload = Buffer.from(protobufSerialize(3, "ping", "2024-01-01T00:00:00.000Z"));
    const output = handleMessage({ payload, offset: 55 });
    expect(output).toContain("offset=55");
  });
});
