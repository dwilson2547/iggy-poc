/**
 * Iggy consumer that deserializes Apache Avro messages.
 *
 * The Avro schema is defined in `src/schemas.ts` (canonical source:
 * `src/schemas/event.avsc`).  Each message payload is treated as raw Avro
 * bytes and decoded back to an {@link EventRecord} object.
 *
 * Usage:
 *   npm run avro:consumer
 */

import { Client, ConsumerKind, PollingStrategy } from "@iggy.rs/sdk";
import { avroDeserialize } from "./schemas.js";

const STREAM_NAME = "demo-stream";
const TOPIC_NAME = "avro-topic";
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
    `Consuming Avro messages from stream='${STREAM_NAME}' topic='${TOPIC_NAME}' ` +
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
  const record = avroDeserialize(message.payload);
  console.log(`[offset=${message.offset}] id=${record.id} text='${record.text}' ts='${record.ts}'`);
}

main().catch((error) => {
  console.error("Fatal error in Avro consumer:", error);
  process.exit(1);
});
