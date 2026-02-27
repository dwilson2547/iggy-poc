# Java SDK Clients for Iggy POC

Java producer and consumer using the official **Apache Iggy Java SDK** with Maven.

## ✅ Current Status

**Server Version:** 0.6.0 (apache/iggy:0.6.0)  
**SDK Version:** 0.6.0 (org.apache.iggy:iggy)  
**Compatibility:** Full - matching TCP protocol

When running against `apache/iggy:edge` (0.7.0-edge), the 0.6.0 SDK can connect and authenticate but fails to send messages due to a protocol change (e.g., error 4033 / invalid message size). Use the REST clients in `clients/java/` or wait for the 0.7.x SDK if you need edge.

**What Works (with 0.6.0 server):**
- ✅ TCP connection to server
- ✅ Authentication (login)
- ✅ Stream creation
- ✅ Topic creation
- ✅ Message sending
- ✅ Message polling

This implementation demonstrates correct usage of the official Apache Iggy Java SDK and works fully when the server and SDK versions are aligned.

## About

This implementation uses the official Apache Iggy SDK that is now part of the main Apache Iggy repository. The SDK connects via TCP (port 8090) and provides a native, high-performance interface to Iggy.

## Prerequisites

- **Java 17+** — required by the Iggy SDK
- **Maven 3.6+** — for dependency management and building

## Build

```bash
cd clients/java-sdk
mvn clean install
```

This will download the Apache Iggy SDK (version 0.6.0) and all dependencies.

## Run the Producer

In one terminal:

```bash
cd clients/java-sdk
mvn exec:java@run-producer
```

You'll see output like:

```text
01:23:45.123 INFO  Producer - Connecting to Iggy server...
01:23:45.234 INFO  Producer - Connected and logged in as iggy.
01:23:45.345 INFO  Producer - Stream 'demo-stream' already exists (id=1).
01:23:45.456 INFO  Producer - Topic 'demo-topic' already exists (id=1).
01:23:45.567 INFO  Producer - Sent message #1: {"id":1,"text":"hello from Java SDK producer","ts":"2024-01-15T01:23:45.678Z"}
```

## Run the Consumer

In another terminal:

```bash
cd clients/java-sdk
mvn exec:java@run-consumer
```

You'll see output like:

```text
01:23:50.123 INFO  Consumer - Connecting to Iggy server...
01:23:50.234 INFO  Consumer - Connected and logged in as iggy.
01:23:50.345 INFO  Consumer - [offset=0] {"id":1,"text":"hello from Java SDK producer","ts":"2024-01-15T01:23:45.678Z"}
```

## How It Works

Both clients use the official Apache Iggy Java SDK to connect via TCP on port **8090** (same as Python clients).

### Producer

1. Connects to server via TCP and logs in using the builder pattern
2. Creates stream and topic if they don't exist (idempotent)
3. Sends JSON messages to partition 1 every 1 second
4. Uses `Message.of()` helper for simple message creation
5. Uses `Partitioning.partitionId()` to target specific partition

### Consumer

1. Connects to server via TCP and logs in using the builder pattern
2. Polls messages from partition 1 using offset-based polling strategy
3. Auto-commits offsets after processing each batch
4. Tracks offset manually and increments after each batch
5. Logs each message with its offset

## Message Format

All clients (Python, Java REST API, Java SDK) use the same JSON payload format:

```json
{
  "id": 123,
  "text": "hello from Java SDK producer",
  "ts": "2024-01-15T01:23:45.678Z"
}
```

This ensures full interoperability — messages can flow between any combination of clients.

## Project Structure

```text
java-sdk/
├── pom.xml                     # Maven configuration
├── README.md                   # This file
├── .gitignore                  # Maven ignores
└── src/
    └── main/
        ├── java/com/example/iggy/
        │   ├── Producer.java   # TCP producer using SDK
        │   └── Consumer.java   # TCP consumer using SDK
        └── resources/
            └── logback.xml     # Logging configuration
```

## Dependencies

- **org.apache.iggy:iggy:0.6.0** — Official Apache Iggy Java SDK
- **SLF4J + Logback** — For logging
- **Jackson** — For JSON serialization

## Key Features

- **Native TCP protocol** — Direct binary protocol for maximum performance
- **Builder pattern** — Fluent API for client configuration
- **Auto-authentication** — `buildAndLogin()` combines connection and authentication
- **Type-safe identifiers** — `StreamId` and `TopicId` wrappers
- **Offset management** — Manual tracking with auto-commit option
- **Partitioning strategies** — Flexible message routing

## Comparison with REST API Implementation

This implementation (`java-sdk/`) differs from the REST API implementation (`java/`):

| Aspect | java-sdk/ (This) | java/ (REST API) |
|--------|------------------|------------------|
| **Protocol** | TCP (port 8090) | HTTP REST (port 3000) |
| **SDK** | Official Apache Iggy SDK | Apache HttpClient |
| **Performance** | Native binary protocol | HTTP overhead |
| **Dependency** | org.apache.iggy:iggy | org.apache.httpcomponents.client5 |
| **Compatibility** | Same as Python clients | REST API only |
| **Use Case** | Production recommended | Good for HTTP-only environments |

## Troubleshooting

**Connection refused:**
- Make sure the Iggy server is running: `cd ../../server && docker compose up -d`
- Verify TCP port 8090 is accessible: `nc -zv localhost 8090`

**Build errors:**
- Ensure you have Java 17+ installed: `java -version`
- Check Maven version: `mvn --version`
- Clear Maven cache if needed: `rm -rf ~/.m2/repository/org/apache/iggy`

**SDK not found:**
- The SDK version 0.6.0 should be available on Maven Central
- Check https://mvnrepository.com/artifact/org.apache.iggy/iggy

## References

- [Apache Iggy Homepage](https://iggy.apache.org)
- [Apache Iggy GitHub](https://github.com/apache/iggy)
- [Java Examples](https://github.com/apache/iggy/tree/master/examples/java)
- [SDK Maven Central](https://mvnrepository.com/artifact/org.apache.iggy/iggy)
