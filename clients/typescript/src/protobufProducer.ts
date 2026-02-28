/**
 * Iggy producer that serializes messages using Protocol Buffers.
 *
 * The Protobuf schema is defined in `src/schemas.ts` (canonical source:
 * `src/schemas/event.proto`).  Messages are sent as Protobuf wire-format bytes
 * so that the consumer can decode them with the same schema.
 *
 * Usage:
 *   npm run protobuf:producer
 */

import { Client, Partitioning } from "@iggy.rs/sdk";
import { protobufSerialize } from "./schemas.js";

const STREAM_NAME = "demo-stream";
const TOPIC_NAME = "protobuf-topic";
const PARTITION_ID = 1;
const SEND_INTERVAL_MS = 1000;

async function main() {
  console.log("Connecting to Iggy server...");
  const client = new Client({
    transport: "TCP",
    options: { port: 8090, host: "127.0.0.1" },
    credentials: { username: "iggy", password: "iggy" },
  });

  console.log("Connected. Logging in...");
  console.log("Logged in as iggy.");

  await produceMessages(client);
}

async function produceMessages(client: Client) {
  console.log(
    `Producing Protobuf messages to stream='${STREAM_NAME}' topic='${TOPIC_NAME}' ` +
    `partition=${PARTITION_ID} every ${SEND_INTERVAL_MS}ms. Press Ctrl+C to stop.`
  );

  let messageId = 0;
  while (true) {
    messageId++;
    const ts = new Date().toISOString();
    const payload = protobufSerialize(messageId, "hello from protobuf producer", ts);

    try {
      await client.message.send({
        streamId: STREAM_NAME,
        topicId: TOPIC_NAME,
        partition: Partitioning.PartitionId(PARTITION_ID),
        messages: [{ id: 0n, payload: Buffer.from(payload), headers: {} }],
      });
      console.log(`Sent Protobuf message #${messageId}: id=${messageId} text='hello from protobuf producer' ts=${ts}`);
    } catch (error) {
      console.error(`Failed to send Protobuf message #${messageId}:`, error);
    }

    await new Promise((resolve) => setTimeout(resolve, SEND_INTERVAL_MS));
  }
}

main().catch((error) => {
  console.error("Fatal error in Protobuf producer:", error);
  process.exit(1);
});
