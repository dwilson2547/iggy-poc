/**
 * Iggy consumer that deserializes Protocol Buffers messages.
 *
 * The Protobuf schema is defined in `src/schemas.ts` (canonical source:
 * `src/schemas/event.proto`).  Each message payload is treated as Protobuf
 * wire-format bytes and decoded back to a plain event object.
 *
 * Usage:
 *   npm run protobuf:consumer
 */

import { Client, ConsumerKind, PollingStrategy } from "@iggy.rs/sdk";
import { protobufDeserialize } from "./schemas.js";

const STREAM_NAME = "demo-stream";
const TOPIC_NAME = "protobuf-topic";
const PARTITION_ID = 1;
const POLL_INTERVAL_MS = 500;
const MESSAGES_PER_BATCH = 10;

async function main() {
  console.log("Connecting to Iggy server...");
  const client = new Client({
    transport: "TCP",
    options: { port: 8090, host: "127.0.0.1" },
    credentials: { username: "iggy", password: "iggy" },
  });

  console.log("Connected. Logging in...");
  console.log("Logged in as iggy.");

  await consumeMessages(client);
}

async function consumeMessages(client: Client) {
  console.log(
    `Consuming Protobuf messages from stream='${STREAM_NAME}' topic='${TOPIC_NAME}' ` +
    `partition=${PARTITION_ID}. Press Ctrl+C to stop.`
  );

  while (true) {
    try {
      const polled = await client.message.poll({
        streamId: STREAM_NAME,
        topicId: TOPIC_NAME,
        partitionId: PARTITION_ID,
        consumer: { kind: ConsumerKind.Single, id: 1 },
        pollingStrategy: PollingStrategy.Next,
        count: MESSAGES_PER_BATCH,
        autocommit: true,
      });

      if (!polled.messages || polled.messages.length === 0) {
        await new Promise((resolve) => setTimeout(resolve, POLL_INTERVAL_MS));
        continue;
      }

      for (const msg of polled.messages) {
        handleMessage(msg);
      }

      await new Promise((resolve) => setTimeout(resolve, POLL_INTERVAL_MS));
    } catch (error) {
      console.error("Error while consuming — retrying:", error);
      await new Promise((resolve) => setTimeout(resolve, POLL_INTERVAL_MS));
    }
  }
}

export function handleMessage(message: { payload: Buffer; offset: bigint | number }) {
  const event = protobufDeserialize(message.payload);
  console.log(`[offset=${message.offset}] id=${event.id} text='${event.text}' ts='${event.ts}'`);
}

main().catch((error) => {
  console.error("Fatal error in Protobuf consumer:", error);
  process.exit(1);
});
