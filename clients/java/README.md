# Java Clients for Iggy POC

Java producer and consumer using Maven and Apache HttpClient to interact with Iggy's HTTP REST API.

## Prerequisites

- **Java 17+** — required for the HTTP client
- **Maven 3.6+** — for dependency management and building

## Architecture

Unlike the Python clients which use the `iggy-py` SDK with TCP protocol (port 8090), these Java clients communicate directly with Iggy's HTTP REST API (port 3000). This approach was chosen because:
- The Java SDK (`iggy-rs/iggy-java-client`) was archived in 2025
- The HTTP REST API is stable and well-documented
- No external SDK dependencies required (uses standard Apache HttpClient)

## Build

```bash
cd clients/java
mvn clean install
```

This will download all dependencies and compile the code.

## Run the Producer

In one terminal:

```bash
cd clients/java
mvn exec:java@run-producer
```

You'll see output like:

```text
01:23:45.123 INFO  Producer - Connecting to Iggy server...
01:23:45.234 INFO  Producer - Logging in...
01:23:45.345 INFO  Producer - Logged in as iggy.
01:23:45.456 INFO  Producer - Stream 'demo-stream' already exists.
01:23:45.567 INFO  Producer - Topic 'demo-topic' already exists.
01:23:45.678 INFO  Producer - Sent message #1: {"id":1,"text":"hello from Java producer","ts":"2024-01-15T01:23:45.678Z"}
```

## Run the Consumer

In another terminal:

```bash
cd clients/java
mvn exec:java@run-consumer
```

You'll see output like:

```text
01:23:50.123 INFO  Consumer - Connecting to Iggy server...
01:23:50.234 INFO  Consumer - Logging in...
01:23:50.345 INFO  Consumer - Logged in as iggy.
01:23:50.456 INFO  Consumer - [offset=0] {"id":1,"text":"hello from Java producer","ts":"2024-01-15T01:23:45.678Z"}
```

## How It Works

Both clients connect to the Iggy server via HTTP REST API on port **3000** (unlike Python clients which use TCP port 8090).

### Producer

1. Authenticates via POST `/users/login` to get a JWT token
2. Creates the stream and topic if they don't exist (idempotent)
3. Sends JSON messages to partition 1 every 1 second via POST `/streams/{stream}/topics/{topic}/messages`
4. Messages are base64-encoded and wrapped in the required envelope format

### Consumer

1. Authenticates via POST `/users/login` to get a JWT token
2. Polls messages from partition 1 using GET `/streams/{stream}/topics/{topic}/messages`
3. Uses offset-based polling strategy to track progress
4. Auto-commits offsets after processing each batch
5. Base64-decodes message payloads and logs them

## Message Format

Messages are sent and received in Iggy's HTTP REST API format:

**Send (POST /streams/{stream}/topics/{topic}/messages):**
```json
{
  "partitioning": {
    "kind": "partition_id",
    "value": "<base64-encoded 4-byte partition ID>"
  },
  "messages": [
    {
      "payload": "<base64-encoded message content>"
    }
  ]
}
```

**Receive (GET /streams/{stream}/topics/{topic}/messages):**
```json
{
  "partition_id": 1,
  "current_offset": 100,
  "messages": [
    {
      "offset": 99,
      "timestamp": 1771397214000000,
      "payload": "<base64-encoded message content>"
    }
  ]
}
```

The Java and Python clients use the same JSON payload format internally:

```json
{
  "id": 123,
  "text": "hello from Java producer",
  "ts": "2024-01-15T01:23:45.678Z"
}
```

This ensures interoperability — you can send from Python and receive in Java, or vice versa.

## Project Structure

```text
java/
├── pom.xml                     # Maven configuration
├── README.md                   # This file
├── .gitignore                  # Maven ignores
└── src/
    └── main/
        ├── java/com/example/iggy/
        │   ├── Producer.java   # Sends messages (1/sec via HTTP)
        │   └── Consumer.java   # Polls messages (every 500ms via HTTP)
        └── resources/
            └── logback.xml     # Logging configuration
```

## Dependencies

- **Apache HttpClient 5.2.1** — For HTTP REST API communication
- **SLF4J + Logback** — For logging
- **Jackson** — For JSON serialization

## Troubleshooting

**Connection refused:**
- Make sure the Iggy server is running: `cd ../../server && docker compose up -d`
- Verify port 3000 is accessible: `nc -zv localhost 3000`

**Authentication failed:**
- Default credentials are `iggy:iggy` — these are hardcoded in the clients

**Maven errors:**
- Ensure you have Java 17+ installed: `java -version`
- Check Maven version: `mvn --version`

## Notes

- The Java clients use HTTP REST API while Python clients use TCP for optimal SDK support
- Both approaches are valid and interoperable at the message level
- Messages flow correctly between Java and Python clients regardless of transport protocol
