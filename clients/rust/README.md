# Rust Clients for Iggy POC

Rust producer and consumer using the official Apache Iggy Rust SDK with Cargo.

## Prerequisites

- **Rust 1.70+** — Install from [rustup.rs](https://rustup.rs/)
- **Cargo** — Comes with Rust installation

## Architecture

This implementation uses the official Apache Iggy Rust SDK (`iggy` crate) that connects via **TCP on port 8090** (same as Python clients). The Rust SDK is the reference implementation maintained as part of the main Apache Iggy project and offers the best performance and feature completeness.

## Build

```bash
cd clients/rust
cargo build --release
```

This will download all dependencies and compile the producer and consumer binaries.

## Run the Producer

In one terminal:

```bash
cd clients/rust
cargo run --bin producer
```

You'll see output like:

```text
INFO  Connecting to Iggy server...
INFO  Connected. Logging in...
INFO  Logged in as iggy.
INFO  Stream 'demo-stream' created or already exists.
INFO  Topic 'demo-topic' created or already exists.
INFO  Sent message #1: {"id":1,"text":"hello from Rust producer","ts":"2024-01-15T01:23:45.678Z"}
```

## Run the Consumer

In another terminal:

```bash
cd clients/rust
cargo run --bin consumer
```

You'll see output like:

```text
INFO  Connecting to Iggy server...
INFO  Connected. Logging in...
INFO  Logged in as iggy.
INFO  [offset=0] {"id":1,"text":"hello from Rust producer","ts":"2024-01-15T01:23:45.678Z"}
```

## How It Works

Both clients use the official Apache Iggy Rust SDK to connect via **TCP on port 8090** (same as Python clients).

### Producer

1. Connects to server via TCP using connection string format
2. Authenticates with username/password
3. Creates stream and topic if they don't exist (idempotent)
4. Sends JSON messages to partition 1 every 1 second
5. Uses `send_message()` API for message production

### Consumer

1. Connects to server via TCP using connection string format
2. Authenticates with username/password
3. Polls messages from partition 1 using next-offset polling strategy
4. Logs each message with its offset

## Message Format

All clients (Python, Java, Java SDK, Rust, TypeScript) use the same JSON payload format:

```json
{
  "id": 123,
  "text": "hello from Rust producer",
  "ts": "2024-01-15T01:23:45.678Z"
}
```

This ensures full interoperability — messages can flow between any combination of clients.

## Project Structure

```text
rust/
├── Cargo.toml              # Cargo configuration with dependencies
├── README.md               # This file
└── src/
    ├── producer.rs         # TCP producer using Rust SDK
    └── consumer.rs         # TCP consumer using Rust SDK
```

## Dependencies

- **iggy 0.8** — Official Apache Iggy Rust SDK
- **tokio** — Async runtime
- **serde + serde_json** — JSON serialization
- **chrono** — Timestamp handling
- **tracing + tracing-subscriber** — Logging

## Key Features

- **Native TCP protocol** — Direct binary protocol for maximum performance
- **Type-safe API** — Rust's type system ensures correctness
- **Async/await support** — Built on tokio for efficient concurrency
- **Idempotent setup** — Stream and topic creation is safe to re-run
- **Connection string format** — Simple configuration via URI

## Comparison with Other Clients

| Aspect | rust/ (This) | python/ | typescript/ | java-sdk/ | java/ (REST) |
|--------|--------------|---------|-------------|-----------|--------------|
| **Protocol** | TCP (port 8090) | TCP (port 8090) | TCP (port 8090) | TCP (port 8090) | HTTP REST (port 3000) |
| **SDK** | Official Rust SDK | iggy-py | Official TypeScript SDK | Official Java SDK | Apache HttpClient |
| **Performance** | Native (fastest) | Native | Native | Native | HTTP overhead |
| **Language** | Rust | Python | TypeScript | Java | Java |
| **Use Case** | Production recommended | Production | Production | Production | HTTP-only environments |

## Troubleshooting

**Connection refused:**
- Make sure the Iggy server is running: `cd ../../server && docker compose up -d`
- Verify TCP port 8090 is accessible: `nc -zv localhost 8090`

**Build errors:**
- Ensure you have Rust installed: `rustc --version`
- Update Rust if needed: `rustup update`
- Clear Cargo cache if needed: `cargo clean`

**SDK not found:**
- The SDK version 0.8 should be available on crates.io
- Check https://crates.io/crates/iggy

## References

- [Apache Iggy Homepage](https://iggy.apache.org)
- [Apache Iggy GitHub](https://github.com/apache/iggy)
- [Rust SDK Documentation](https://docs.rs/iggy/latest/iggy/)
- [Rust Examples](https://github.com/apache/iggy/tree/master/examples/rust)
