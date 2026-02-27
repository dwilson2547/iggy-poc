import { Client } from "@iggy.rs/sdk";

const STREAM_NAME = "demo-stream";
const TOPIC_NAME = "demo-topic";
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
    `Consuming from stream='${STREAM_NAME}' topic='${TOPIC_NAME}' partition=${PARTITION_ID}. Press Ctrl+C to stop.`
  );

  while (true) {
    try {
      const polled = await client.message.poll({
        streamId: STREAM_NAME,
        topicId: TOPIC_NAME,
        partitionId: PARTITION_ID,
        consumer: { kind: "Consumer", id: 1 },
        pollingStrategy: { kind: "Next", value: 0n },
        count: MESSAGES_PER_BATCH,
        autocommit: true,
      });

      if (!polled.messages || polled.messages.length === 0) {
        // No new messages — wait before polling again
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

function handleMessage(message: any) {
  const payload = message.payload.toString("utf-8");
  console.log(`[offset=${message.offset}] ${payload}`);
}

main().catch((error) => {
  console.error("Fatal error in consumer:", error);
  process.exit(1);
});
