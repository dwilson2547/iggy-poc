import { describe, it, expect } from "vitest";
import { avroSerialize, avroDeserialize, type EventRecord } from "../src/schemas.js";

describe("Avro serialization (schemas.ts)", () => {
  it("roundtrip produces the original record", () => {
    const record: EventRecord = {
      id: 1,
      text: "hello from avro producer",
      ts: "2024-01-01T00:00:00.000Z",
    };

    const bytes = avroSerialize(record);
    const result = avroDeserialize(bytes);

    expect(result.id).toBe(1);
    expect(result.text).toBe("hello from avro producer");
    expect(result.ts).toBe("2024-01-01T00:00:00.000Z");
  });

  it("serialized output is a non-empty Buffer", () => {
    const bytes = avroSerialize({ id: 42, text: "test", ts: "2024-01-01T00:00:00.000Z" });
    expect(bytes).toBeInstanceOf(Buffer);
    expect(bytes.length).toBeGreaterThan(0);
  });

  it("different records produce different byte sequences", () => {
    const a = avroSerialize({ id: 1, text: "first",  ts: "2024-01-01T00:00:00.000Z" });
    const b = avroSerialize({ id: 2, text: "second", ts: "2024-01-01T00:00:00.000Z" });
    expect(a.equals(b)).toBe(false);
  });

  it("successive records carry incrementing ids after roundtrip", () => {
    const ts = new Date().toISOString();
    const ids = [1, 2, 3].map((i) =>
      avroDeserialize(avroSerialize({ id: i, text: "msg", ts })).id
    );
    expect(ids).toEqual([1, 2, 3]);
  });
});

describe("Avro consumer message handling", () => {
  // Mirrors avroConsumer.handleMessage without importing the file that calls main().
  function handleMessage(message: { payload: Buffer; offset: number | bigint }): string {
    const record = avroDeserialize(message.payload);
    return `[offset=${message.offset}] id=${record.id} text='${record.text}' ts='${record.ts}'`;
  }

  it("decodes Avro bytes and includes event fields in output", () => {
    const record: EventRecord = { id: 5, text: "avro-test", ts: "2024-06-01T12:00:00.000Z" };
    const payload = avroSerialize(record);

    const output = handleMessage({ payload, offset: 10 });

    expect(output).toContain("avro-test");
    expect(output).toContain("offset=10");
    expect(output).toContain("id=5");
  });

  it("handles messages at arbitrary offsets", () => {
    const payload = avroSerialize({ id: 99, text: "offset-test", ts: "2024-01-01T00:00:00.000Z" });
    const output = handleMessage({ payload, offset: 42 });
    expect(output).toContain("offset=42");
  });
});
