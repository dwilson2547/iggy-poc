import { Client } from "@iggy.rs/sdk";

const STREAM_NAME = "demo-stream";
const TOPIC_NAME = "demo-topic";
const PARTITION_ID = 1;
const SEND_INTERVAL_MS = 1000;

interface MessagePayload {
  id: number;
  text: string;
  ts: string;
}

async function main() {
  console.log("Connecting to Iggy server...");
  const client = new Client({
    transport: "TCP",
    options: { port: 8090, host: "127.0.0.1" },
    credentials: { username: "iggy", password: "iggy" },
  });

  console.log("Connected. Logging in...");
  console.log("Logged in as iggy.");

  // Note: Stream and topic should be created by running Python producer first,
  // or manually via CLI. The TypeScript SDK create methods have API compatibility issues.
  
  await produceMessages(client);
}

async function produceMessages(client: Client) {
  console.log(
    `Producing to stream='${STREAM_NAME}' topic='${TOPIC_NAME}' partition=${PARTITION_ID} every ${SEND_INTERVAL_MS}ms. Press Ctrl+C to stop.`
  );

  let messageId = 0;
  while (true) {
    messageId++;

    const payload: MessagePayload = {
      id: messageId,
      text: "hello from TypeScript producer",
      ts: new Date().toISOString(),
    };

    const jsonPayload = JSON.stringify(payload);

    try {
      await client.message.send({
        streamId: STREAM_NAME,
        topicId: TOPIC_NAME,
        partitioning: {
          kind: "PartitionId",
          value: PARTITION_ID,
        },
        messages: [
          {
            id: 0n,
            payload: Buffer.from(jsonPayload),
            headers: {},
          },
        ],
      });
      console.log(`Sent message #${messageId}: ${jsonPayload}`);
    } catch (error) {
      console.error(`Failed to send message #${messageId}:`, error);
    }

    await new Promise((resolve) => setTimeout(resolve, SEND_INTERVAL_MS));
  }
}

main().catch((error) => {
  console.error("Fatal error in producer:", error);
  process.exit(1);
});
