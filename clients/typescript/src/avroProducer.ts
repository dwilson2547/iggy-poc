/**
 * Iggy producer that serializes messages using Apache Avro.
 *
 * The Avro schema is defined in `src/schemas.ts` (canonical source:
 * `src/schemas/event.avsc`).  Messages are sent as raw Avro bytes so that the
 * consumer can decode them with the same schema.
 *
 * Usage:
 *   npm run avro:producer
 */

import { Client, Partitioning } from "@iggy.rs/sdk";
import { avroSerialize } from "./schemas.js";

const STREAM_NAME = "demo-stream";
const TOPIC_NAME = "avro-topic";
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
    `Producing Avro messages to stream='${STREAM_NAME}' topic='${TOPIC_NAME}' ` +
    `partition=${PARTITION_ID} every ${SEND_INTERVAL_MS}ms. Press Ctrl+C to stop.`
  );

  let messageId = 0;
  while (true) {
    messageId++;
    const ts = new Date().toISOString();
    const payload = avroSerialize({ id: messageId, text: "hello from avro producer", ts });

    try {
      await client.message.send({
        streamId: STREAM_NAME,
        topicId: TOPIC_NAME,
        partition: Partitioning.PartitionId(PARTITION_ID),
        messages: [{ id: 0n, payload, headers: {} }],
      });
      console.log(`Sent Avro message #${messageId}: id=${messageId} text='hello from avro producer' ts=${ts}`);
    } catch (error) {
      console.error(`Failed to send Avro message #${messageId}:`, error);
    }

    await new Promise((resolve) => setTimeout(resolve, SEND_INTERVAL_MS));
  }
}

main().catch((error) => {
  console.error("Fatal error in Avro producer:", error);
  process.exit(1);
});
