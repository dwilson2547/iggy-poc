# TypeScript Clients for Iggy POC

TypeScript producer and consumer using the official Apache Iggy TypeScript SDK with npm.

## Prerequisites

- **Node.js 18+** — Install from [nodejs.org](https://nodejs.org/)
- **npm** — Comes with Node.js installation

## Architecture

This implementation uses the official Apache Iggy TypeScript SDK (`@iggy.rs/sdk` npm package) that connects via **TCP on port 8090** (same as Python clients). The TypeScript SDK provides a type-safe interface to interact with Iggy and offers native performance through TCP protocol.

## Install Dependencies

```bash
cd clients/typescript
npm install
```

This will download the Iggy SDK and all dependencies.

## Prerequisites

Before running the TypeScript clients, **create the stream and topic** using either:

**Option A: Python client**
```bash
cd ../python
python3 -m venv venv
source venv/bin/activate
pip install iggy-py loguru
timeout 3 python3 producer.py  # This creates stream and topic
```

**Option B: REST API**
```bash
TOKEN=$(curl -s -X POST http://localhost:3000/users/login -H 'Content-Type: application/json' -d '{"username":"iggy","password":"iggy"}' | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token']['token'])")

curl -X POST -H "Authorization: ******" -H "Content-Type: application/json" -d '{"name":"demo-stream"}' http://localhost:3000/streams

curl -X POST -H "Authorization: ******" -H "Content-Type: application/json" -d '{"name":"demo-topic","partitions_count":1,"compression_algorithm":"none","message_expiry":0,"max_topic_size":0,"replication_factor":1}' http://localhost:3000/streams/demo-stream/topics
```

> **Note**: The TypeScript SDK 1.0.6 has API compatibility issues with stream/topic creation methods. The producer and consumer work correctly once the stream and topic exist.

## Run the Producer

In one terminal:

```bash
cd clients/typescript
npm run producer
```

You'll see output like:

```text
Connecting to Iggy server...
Connected. Logging in...
Logged in as iggy.
Producing to stream='demo-stream' topic='demo-topic' partition=1 every 1000ms. Press Ctrl+C to stop.
```

Messages are being sent successfully via TCP to the Iggy server.

## Run the Consumer

> **Note**: The consumer currently has an SDK compatibility issue being investigated. The producer demonstrates TCP connectivity successfully.

```bash
cd clients/typescript
npm run consumer
```

## How It Works

Both clients use the official Apache Iggy TypeScript SDK to connect via **TCP on port 8090** (same as Python clients).

### Producer

1. Connects to server via TCP and authenticates
2. Sends JSON messages to partition 1 every 1 second
3. Uses `client.message.send()` with Buffer payload
4. Uses `partitioning.kind = "PartitionId"` to target specific partition

### Consumer (Work in Progress)

1. Connects to server via TCP and authenticates  
2. Polls messages from partition 1 using next-offset polling strategy
3. Should auto-commit offsets after processing each batch
4. Should log each message with its offset

## Message Format

All clients (Python, Java, Java SDK, TypeScript) use the same JSON payload format:

```json
{
  "id": 123,
  "text": "hello from TypeScript producer",
  "ts": "2024-01-15T01:23:45.678Z"
}
```

This ensures full interoperability — messages can flow between any combination of clients.

## Project Structure

```text
typescript/
├── package.json            # npm configuration with dependencies
├── tsconfig.json           # TypeScript configuration
├── README.md               # This file
└── src/
    ├── producer.ts         # TCP producer using TypeScript SDK (working)
    └── consumer.ts         # TCP consumer using TypeScript SDK (WIP)
```

## Dependencies

- **@iggy.rs/sdk** — Official Apache Iggy TypeScript SDK (version 1.0.6)
- **tsx** — TypeScript execution tool (dev dependency)
- **typescript** — TypeScript compiler (dev dependency)

## Key Features

- **Native TCP protocol** — Direct binary protocol for high performance (port 8090)
- **Type-safe API** — TypeScript provides compile-time type checking
- **Async/await support** — Built on Promises for efficient concurrency
- **Zero external runtime dependencies** — Only dev dependencies for TypeScript tooling

## Current Status

✅ **Producer**: Fully working - successfully sends messages via TCP  
⚠️ **Consumer**: API compatibility issues with SDK 1.0.6 being investigated

The producer demonstrates successful TCP connectivity to Iggy, which was the primary goal of this POC.

## Troubleshooting

**Connection refused:**
- Make sure the Iggy server is running: `cd ../../server && docker compose up -d`
- Verify TCP port 8090 is accessible: `nc -zv localhost 8090`

**Module not found:**
- Make sure you ran `npm install` first
- Check Node.js version: `node --version` (should be 18+)

**SDK not found:**
- The SDK should be available on npm
- Check https://www.npmjs.com/package/@iggy.rs/sdk

## References

- [Apache Iggy Homepage](https://iggy.apache.org)
- [Apache Iggy GitHub](https://github.com/apache/iggy)
- [TypeScript SDK GitHub](https://github.com/iggy-rs/iggy-node-client)
