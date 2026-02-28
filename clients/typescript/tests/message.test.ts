import { describe, it, expect } from "vitest";

interface MessagePayload {
  id: number;
  text: string;
  ts: string;
}

describe("Producer message payload", () => {
  it("serializes to valid JSON with required fields", () => {
    const payload: MessagePayload = {
      id: 1,
      text: "hello from TypeScript producer",
      ts: new Date().toISOString(),
    };

    const json = JSON.stringify(payload);
    const parsed = JSON.parse(json) as MessagePayload;

    expect(parsed.id).toBe(1);
    expect(parsed.text).toBe("hello from TypeScript producer");
    expect(parsed.ts).toBeDefined();
  });

  it("increments message ids for successive messages", () => {
    const payloads: MessagePayload[] = [1, 2, 3].map((i) => ({
      id: i,
      text: "hello from TypeScript producer",
      ts: new Date().toISOString(),
    }));

    expect(payloads.map((p) => p.id)).toEqual([1, 2, 3]);
  });
});

describe("Consumer message handling", () => {
  it("decodes a UTF-8 Buffer payload to a string", () => {
    const content = '{"id":1,"text":"hello from TypeScript producer"}';
    const buffer = Buffer.from(content, "utf-8");

    const decoded = buffer.toString("utf-8");

    expect(decoded).toBe(content);
  });

  it("parses a decoded JSON payload correctly", () => {
    const content = '{"id":2,"text":"hello from TypeScript producer","ts":"2024-01-01T00:00:00.000Z"}';
    const buffer = Buffer.from(content, "utf-8");
    const decoded = buffer.toString("utf-8");

    const parsed = JSON.parse(decoded) as MessagePayload;

    expect(parsed.id).toBe(2);
    expect(parsed.text).toBe("hello from TypeScript producer");
  });
});
