# Iggy POC Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a working Apache Iggy demo with a Docker-hosted server, a Python async producer, a Python async consumer, and comprehensive documentation covering all core Iggy concepts.

**Architecture:** Iggy server runs via Docker Compose (TCP:8090, HTTP:8080, WS:3000). Two standalone Python scripts (`producer.py`, `consumer.py`) connect via TCP using the `iggy-py` SDK. The producer sends one JSON message per second to `demo-stream/demo-topic`. The consumer polls for new messages every 500 ms and prints each to stdout.

**Tech Stack:** Docker Compose, `iggyrs/iggy:latest` image, Python 3.10+, `iggy-py>=0.4.0`, `loguru>=0.7.0`

---

## Task 1: Iggy Server — Docker Compose

**Files:**
- Create: `server/docker-compose.yml`

**Step 1: Create the compose file**

```yaml
# server/docker-compose.yml
services:
  iggy:
    image: iggyrs/iggy:latest
    container_name: iggy-server
    ports:
      - "8090:8090"   # TCP binary (Python SDK)
      - "8080:8080"   # HTTP REST (curl inspection)
      - "3000:3000"   # WebSocket
    volumes:
      - iggy-data:/iggy/data
    environment:
      IGGY_ROOT_PASSWORD: iggy
    restart: unless-stopped

volumes:
  iggy-data:
```

**Step 2: Start the server and verify it's up**

```bash
cd server
docker compose up -d
docker compose logs iggy
```

Expected: logs show `Iggy server started` and all three transports are listening. Give it 5–10 seconds.

**Step 3: Smoke-test the HTTP API**

```bash
curl http://localhost:8080/api/streams
```

Expected: `[]` (empty JSON array — no streams yet, but server is responding).

---

## Task 2: Python Client Dependencies

**Files:**
- Create: `clients/requirements.txt`

**Step 1: Create requirements.txt**

```
iggy-py>=0.4.0
loguru>=0.7.0
```

**Step 2: Create and activate a virtualenv, install dependencies**

```bash
cd clients
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Expected: both packages install without errors. Verify:

```bash
python -c "import iggy_py; print('iggy-py ok')"
```

Expected: `iggy-py ok`

---

## Task 3: Producer

**Files:**
- Create: `clients/producer.py`

**Step 1: Write producer.py**

```python
import asyncio
import json
from datetime import datetime, timezone
from loguru import logger
from iggy_py import IggyClient, SendMessage as Message, StreamDetails, TopicDetails

STREAM_NAME = "demo-stream"
TOPIC_NAME = "demo-topic"
PARTITION_ID = 1
SEND_INTERVAL_SECS = 1.0


async def main() -> None:
    client = IggyClient()
    logger.info("Connecting to Iggy server...")
    await client.connect()
    logger.info("Connected. Logging in...")
    await client.login_user("iggy", "iggy")
    logger.info("Logged in as iggy.")
    await init_system(client)
    await produce_messages(client)


async def init_system(client: IggyClient) -> None:
    """Idempotently create the stream and topic if they don't exist."""
    try:
        stream: StreamDetails = await client.get_stream(STREAM_NAME)
        if stream is None:
            await client.create_stream(name=STREAM_NAME)
            logger.info(f"Stream '{STREAM_NAME}' created.")
        else:
            logger.info(f"Stream '{STREAM_NAME}' already exists (id={stream.id}).")
    except Exception as e:
        logger.error(f"Error setting up stream: {e}")
        raise

    try:
        topic: TopicDetails = await client.get_topic(STREAM_NAME, TOPIC_NAME)
        if topic is None:
            await client.create_topic(
                stream=STREAM_NAME,
                partitions_count=1,
                name=TOPIC_NAME,
                replication_factor=1,
            )
            logger.info(f"Topic '{TOPIC_NAME}' created.")
        else:
            logger.info(f"Topic '{TOPIC_NAME}' already exists (id={topic.id}).")
    except Exception as e:
        logger.error(f"Error setting up topic: {e}")
        raise


async def produce_messages(client: IggyClient) -> None:
    logger.info(
        f"Producing to stream='{STREAM_NAME}' topic='{TOPIC_NAME}' "
        f"partition={PARTITION_ID} every {SEND_INTERVAL_SECS}s. Press Ctrl+C to stop."
    )
    message_id = 0
    while True:
        message_id += 1
        payload = json.dumps({
            "id": message_id,
            "text": "hello from producer",
            "ts": datetime.now(timezone.utc).isoformat(),
        })
        message = Message(payload)
        try:
            await client.send_messages(
                stream=STREAM_NAME,
                topic=TOPIC_NAME,
                partitioning=PARTITION_ID,
                messages=[message],
            )
            logger.info(f"Sent message #{message_id}: {payload}")
        except Exception as e:
            logger.error(f"Failed to send message #{message_id}: {e}")
        await asyncio.sleep(SEND_INTERVAL_SECS)


if __name__ == "__main__":
    asyncio.run(main())
```

**Step 2: Run the producer and verify messages are sent**

Make sure the Iggy server is up (Task 1), then in the `clients/` directory with the virtualenv active:

```bash
python producer.py
```

Expected output (one line per second):
```
2026-... INFO  Connecting to Iggy server...
2026-... INFO  Connected. Logging in...
2026-... INFO  Logged in as iggy.
2026-... INFO  Stream 'demo-stream' created.
2026-... INFO  Topic 'demo-topic' created.
2026-... INFO  Producing to stream='demo-stream'...
2026-... INFO  Sent message #1: {"id": 1, "text": "hello from producer", ...}
2026-... INFO  Sent message #2: ...
```

**Step 3: Verify via HTTP API that messages exist**

In a separate terminal:

```bash
curl http://localhost:8080/api/streams
curl "http://localhost:8080/api/streams/demo-stream/topics/demo-topic"
```

Expected: stream and topic details, non-zero message count.

Leave the producer running and move to Task 4.

---

## Task 4: Consumer

**Files:**
- Create: `clients/consumer.py`

**Step 1: Write consumer.py**

```python
import asyncio
from loguru import logger
from iggy_py import IggyClient, ReceiveMessage, PollingStrategy

STREAM_NAME = "demo-stream"
TOPIC_NAME = "demo-topic"
PARTITION_ID = 1
POLL_INTERVAL_SECS = 0.5
MESSAGES_PER_BATCH = 10


async def main() -> None:
    client = IggyClient()
    logger.info("Connecting to Iggy server...")
    await client.connect()
    logger.info("Connected. Logging in...")
    await client.login_user("iggy", "iggy")
    logger.info("Logged in as iggy.")
    await consume_messages(client)


async def consume_messages(client: IggyClient) -> None:
    logger.info(
        f"Consuming from stream='{STREAM_NAME}' topic='{TOPIC_NAME}' "
        f"partition={PARTITION_ID}. Press Ctrl+C to stop."
    )
    while True:
        try:
            polled: list[ReceiveMessage] = await client.poll_messages(
                stream=STREAM_NAME,
                topic=TOPIC_NAME,
                partition_id=PARTITION_ID,
                polling_strategy=PollingStrategy.Next(),
                count=MESSAGES_PER_BATCH,
                auto_commit=True,
            )
            if not polled:
                logger.debug("No new messages — waiting...")
                await asyncio.sleep(POLL_INTERVAL_SECS)
                continue

            for msg in polled:
                handle_message(msg)

            await asyncio.sleep(POLL_INTERVAL_SECS)
        except Exception as e:
            logger.exception(f"Error while consuming: {e}")
            break


def handle_message(message: ReceiveMessage) -> None:
    payload = message.payload().decode("utf-8")
    logger.info(f"[offset={message.offset()}] {payload}")


if __name__ == "__main__":
    asyncio.run(main())
```

**Step 2: Run the consumer in a separate terminal (producer still running)**

```bash
cd clients
source .venv/bin/activate
python consumer.py
```

Expected: Consumer connects, then prints each message as the producer sends them. Offsets should increment from 0.

```
2026-... INFO  Connecting to Iggy server...
2026-... INFO  Logged in as iggy.
2026-... INFO  Consuming from stream='demo-stream'...
2026-... INFO  [offset=0] {"id": 1, "text": "hello from producer", ...}
2026-... INFO  [offset=1] {"id": 2, ...}
```

**Step 3: Verify offset persistence**

Stop the consumer (Ctrl+C), wait a few seconds for more messages to arrive, then restart it.

Expected: Consumer picks up from where it left off (not from offset 0), because `auto_commit=True` tracks position server-side via `PollingStrategy.Next()`.

---

## Task 5: Iggy Concepts Documentation

**Files:**
- Create: `docs/iggy-concepts.md`

**Step 1: Write the concepts guide**

```markdown
# Apache Iggy — Core Concepts

Apache Iggy is a **persistent message streaming platform** written in Rust. It is designed for high-throughput, low-latency message delivery and competes in the space occupied by Apache Kafka, NATS JetStream, and RabbitMQ Streams — but with a much smaller operational footprint and no JVM or external coordination service (no Zookeeper).

---

## 1. What Is Iggy?

Iggy stores messages durably on disk in an ordered log, just like Kafka. Unlike Kafka it:

- Has **no Zookeeper or KRaft dependency** — a single binary is a fully functional broker.
- Is written in **Rust** using `io_uring` (Linux) for near-zero-copy I/O.
- Supports **multiple transports** from the same server: TCP binary, HTTP REST, QUIC, WebSocket.
- Uses a **thread-per-core, shared-nothing** architecture for predictable latency.

Benchmark numbers from the project: sustained throughput above 2 GB/s with P99 latencies in the low milliseconds on commodity hardware.

---

## 2. Streams

A **stream** is the top-level organisational unit — roughly equivalent to a Kafka "namespace" or an NATS JetStream "stream". It groups related topics together.

- Streams are identified by a unique **name** (string) and an auto-assigned **numeric ID**.
- A single Iggy server can host many streams.
- Streams are created explicitly; they do not auto-create on first use.

```python
await client.create_stream(name="orders")
```

---

## 3. Topics

A **topic** lives inside a stream and is the actual channel through which messages flow. It is an append-only ordered log.

- Each topic belongs to exactly one stream.
- Topics have a configurable number of **partitions** (set at creation time).
- Topics can have a **retention policy** (time- or size-based) to bound disk usage.

```python
await client.create_topic(
    stream="orders",
    name="order-events",
    partitions_count=4,
    replication_factor=1,
)
```

---

## 4. Partitions

A **partition** is a subdivision of a topic. Each partition is an independent ordered log with its own sequence of offsets starting at 0.

**Why partitions matter:**
- Partitions enable **parallelism** — multiple consumers can each read a different partition simultaneously.
- Within a partition, message order is strictly preserved.
- Across partitions, there is no ordering guarantee.

**Partition selection when producing:**

The `partitioning` argument on `send_messages` controls which partition a message lands in:
- A specific integer (e.g. `1`) sends to that partition directly.
- Round-robin or key-based routing are also supported.

---

## 5. Offsets and Polling Strategies

Every message in a partition has a monotonically increasing **offset** (starting at 0). Consumers use offsets to track their position.

### Polling Strategies

| Strategy | Meaning |
|---|---|
| `PollingStrategy.Next()` | Resume from where this consumer last left off (server-tracked). |
| `PollingStrategy.Offset(n)` | Start at a specific absolute offset. |
| `PollingStrategy.First()` | Start from offset 0 (replay all messages). |
| `PollingStrategy.Last()` | Start from the newest unread message. |

**`auto_commit=True`** tells Iggy to automatically advance the server-side offset cursor after each successful poll. This is the default for simple consumers. Set it to `False` if you need manual commit control (e.g. process-then-ack semantics).

---

## 6. Consumer Groups

A **consumer group** allows multiple consumer instances to **share the work** of consuming a topic. Iggy tracks partition assignments server-side.

- Each partition is assigned to at most one consumer in the group at a time.
- If a consumer leaves, its partitions are rebalanced to the remaining members.
- This is the foundation of horizontal scaling for consumers.

Consumer groups are created and joined explicitly:

```python
await client.create_consumer_group(
    stream="orders",
    topic="order-events",
    name="payment-service",
)
await client.join_consumer_group(
    stream="orders",
    topic="order-events",
    consumer_group="payment-service",
)
```

Once joined, poll calls automatically consume from the group's assigned partitions.

---

## 7. Transport Protocols

Iggy exposes the same logical API over four transports:

| Transport | Default Port | Use Case |
|---|---|---|
| TCP binary | 8090 | Production clients (lowest latency, binary framing) |
| HTTP REST | 8080 | Debugging, curl inspection, language-agnostic |
| QUIC | 8080 | Low-latency over UDP; requires TLS setup |
| WebSocket | 3000 | Browser clients, real-time dashboards |

The Python `iggy-py` SDK uses the **TCP binary** protocol. The HTTP API is useful for ad-hoc inspection without a dedicated SDK.

---

## 8. Persistence Model

Iggy writes messages to **segment files** on disk. Key properties:

- Messages are appended sequentially, making writes very fast (sequential I/O).
- Each partition has its own set of segment files.
- Older segments are removed according to the topic's **retention policy** (default: retain forever until disk pressure triggers cleanup).
- On restart, the server recovers state from disk — no data is lost.

The named Docker volume `iggy-data` in this demo preserves all data across container restarts.

---

## 9. Key Differences From Apache Kafka

| Aspect | Kafka | Iggy |
|---|---|---|
| Runtime | JVM (Java/Scala) | Native (Rust) |
| Coordination | Zookeeper or KRaft | None — single binary |
| Protocol | Kafka Wire Protocol | Custom binary, HTTP, QUIC, WS |
| Memory footprint | 512 MB+ minimum | ~10–20 MB |
| Replication | Multi-broker cluster | Supported (planned/partial) |
| Ecosystem maturity | Very mature | Early-stage, rapidly evolving |
| Use case sweet spot | Large-scale enterprise | Edge, embedded, greenfield, high-perf |

---

## Further Reading

- [Apache Iggy homepage](https://iggy.apache.org)
- [Official documentation](https://iggy.apache.org/docs)
- [Python SDK (iggy-py)](https://github.com/iggy-rs/iggy-python-client)
- [GitHub (server)](https://github.com/apache/iggy)
```

**Step 2: Verify the file renders correctly**

Open `docs/iggy-concepts.md` in a Markdown viewer or run:

```bash
wc -l docs/iggy-concepts.md
```

Expected: 150+ lines. No placeholder sections.

---

## Task 6: Architecture Documentation

**Files:**
- Create: `docs/architecture.md`

**Step 1: Write architecture.md**

```markdown
# Apache Iggy — Architecture Overview

This document describes how the Iggy server works internally and how the Python SDK connects to it. It complements [iggy-concepts.md](iggy-concepts.md), which covers the logical model.

---

## Server Internals

```
┌──────────────────────────────────────────────────────────────────────┐
│                          Iggy Server Process                         │
│                                                                      │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │                  Transport Layer (per-thread)                 │   │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐   │   │
│  │  │  TCP binary  │  │  HTTP REST   │  │  QUIC / WebSocket│   │   │
│  │  │   :8090      │  │   :8080      │  │    :8080 / :3000 │   │   │
│  │  └──────┬───────┘  └──────┬───────┘  └────────┬─────────┘   │   │
│  └─────────┼──────────────── ┼──────────────────  ┼─────────────┘   │
│            │                 │                     │                  │
│  ┌─────────▼─────────────────▼─────────────────── ▼─────────────┐   │
│  │                     Command Dispatcher                         │   │
│  │  (deserialise → validate → route to stream/topic handler)      │   │
│  └─────────────────────────────┬──────────────────────────────────┘  │
│                                │                                      │
│  ┌─────────────────────────────▼──────────────────────────────────┐  │
│  │                     Storage Engine                              │  │
│  │                                                                 │  │
│  │  Stream A            Stream B                                   │  │
│  │  └── Topic X         └── Topic Y                               │  │
│  │      ├── Partition 1     ├── Partition 1                       │  │
│  │      │   ├── seg-0.log  │   ├── seg-0.log                     │  │
│  │      │   └── seg-1.log  │   └── seg-1.log                     │  │
│  │      └── Partition 2    └── Partition 2                        │  │
│  └─────────────────────────────────────────────────────────────── ┘  │
└──────────────────────────────────────────────────────────────────────┘
         │ disk I/O via io_uring (Linux async I/O, zero-copy)
         ▼
    /iggy/data/  (bind-mounted volume)
```

### Thread-Per-Core Model

Iggy runs one OS thread per CPU core, with no shared state between threads. Each thread owns a set of TCP connections and processes I/O requests independently using `io_uring` (Linux kernel async I/O interface). This eliminates lock contention under high load and delivers predictable P99 latency.

### Segment Storage

Each partition's messages are stored in **segment files** (`.log`). When a segment reaches its configured size limit, a new one is created. The segment index accelerates seeking to a specific offset without scanning the entire file. On startup, Iggy replays segment metadata to rebuild its in-memory index.

---

## Python SDK Connection Flow

```
producer.py / consumer.py
        │
        │  1. IggyClient()            ← construct client (default: localhost:8090 TCP)
        │  2. await client.connect()  ← open TCP socket, complete handshake
        │  3. await client.login_user("iggy", "iggy")  ← authenticate
        │
        │  TCP binary protocol (custom framing):
        │  ┌──────────────────────────────────────────────────────┐
        │  │  [4-byte length] [1-byte command-id] [payload bytes] │
        │  └──────────────────────────────────────────────────────┘
        │
        ▼
  Iggy Server :8090
```

### Authentication

Iggy uses username/password authentication. The default credentials (`iggy` / `iggy`) are set via the `IGGY_ROOT_PASSWORD` environment variable in Docker. In production, create dedicated user accounts and rotate credentials.

### Message Flow: Produce

```
producer.py
  └── client.send_messages(stream, topic, partitioning=1, messages=[Message(payload)])
        │
        │  Binary frame: SEND_MESSAGES command
        │  Contains: stream ID, topic ID, partition ID, message count, payloads
        ▼
  Iggy server appends to partition segment file
  Returns: acknowledgement frame (success/error)
```

### Message Flow: Consume

```
consumer.py
  └── client.poll_messages(stream, topic, partition_id, PollingStrategy.Next(), count=10)
        │
        │  Binary frame: POLL_MESSAGES command
        │  Server looks up consumer's stored offset, reads `count` messages
        │  If auto_commit=True: server advances stored offset after read
        ▼
  Returns: list[ReceiveMessage]
    └── message.offset()  → absolute position in partition
    └── message.payload() → raw bytes (decode as UTF-8 for this demo)
```

---

## Port Reference

| Port | Protocol | Purpose | Example |
|------|----------|---------|---------|
| 8090 | TCP | Python SDK, all SDK clients | `iggy-py`, Rust client, Go client |
| 8080 | HTTP | REST API for inspection/management | `curl http://localhost:8080/api/streams` |
| 3000 | WebSocket | Browser or WS clients | `ws://localhost:3000` |

---

## Data Volume Layout

```
/iggy/data/
├── state/          ← server state and consumer group offsets
└── streams/
    └── 1/          ← stream id
        └── topics/
            └── 1/  ← topic id
                └── partitions/
                    └── 1/      ← partition id
                        ├── 00000000000000000000.log   ← segment 0
                        ├── 00000000000000000000.index
                        └── ...
```

The named Docker volume `iggy-data` maps to `/iggy/data` inside the container, persisting all data across `docker compose down` and `docker compose up` cycles.
```

**Step 2: Verify the file**

```bash
wc -l docs/architecture.md
```

Expected: 100+ lines with complete ASCII diagrams.

---

## Task 7: README

**Files:**
- Create: `README.md`

**Step 1: Write README.md**

```markdown
# Apache Iggy POC

A minimal working example of [Apache Iggy](https://iggy.apache.org) — a hyper-efficient persistent message streaming platform written in Rust.

This POC demonstrates **simple pub/sub messaging** using:
- An Iggy server running in Docker
- A Python async **producer** that sends JSON messages via TCP
- A Python async **consumer** that polls and prints those messages

---

## Prerequisites

| Tool | Version | Notes |
|------|---------|-------|
| Docker + Docker Compose | Any recent | For the Iggy server |
| Python | 3.10+ | For the clients |

---

## Quick Start

### 1. Start the Iggy server

```bash
cd server
docker compose up -d
```

Verify it's running:

```bash
curl http://localhost:8080/api/streams
# → []  (empty — no streams yet, but server is up)
```

### 2. Install Python dependencies

```bash
cd clients
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Run the producer

In one terminal:

```bash
cd clients && source .venv/bin/activate
python producer.py
```

You'll see one message sent per second:
```
INFO  Sent message #1: {"id": 1, "text": "hello from producer", "ts": "..."}
INFO  Sent message #2: ...
```

### 4. Run the consumer

In a second terminal:

```bash
cd clients && source .venv/bin/activate
python consumer.py
```

You'll see each message as it arrives:
```
INFO  [offset=0] {"id": 1, "text": "hello from producer", "ts": "..."}
INFO  [offset=1] {"id": 2, ...}
```

---

## Project Layout

```
iggy-poc/
├── README.md
├── server/
│   └── docker-compose.yml    # Iggy server (TCP:8090, HTTP:8080, WS:3000)
├── clients/
│   ├── requirements.txt      # iggy-py, loguru
│   ├── producer.py           # Async producer — 1 msg/sec
│   └── consumer.py           # Async consumer — polls every 500ms
└── docs/
    ├── iggy-concepts.md      # Core concept reference (read this first)
    └── architecture.md       # Server internals + SDK connection flow
```

---

## Inspect With curl

The HTTP API on port 8080 lets you inspect the server state without a client:

```bash
# List all streams
curl http://localhost:8080/api/streams

# Get topic details (includes message count)
curl http://localhost:8080/api/streams/demo-stream/topics/demo-topic

# Get partition details (includes current offset)
curl http://localhost:8080/api/streams/demo-stream/topics/demo-topic/partitions
```

---

## Key Concepts

New to Iggy? Start with [`docs/iggy-concepts.md`](docs/iggy-concepts.md):

- **Streams** — top-level namespace
- **Topics** — ordered message log
- **Partitions** — parallelism unit with per-offset tracking
- **Polling strategies** — `Next`, `Offset(n)`, `First`, `Last`
- **Consumer groups** — server-side load balancing across consumers

For server internals and how the SDK connects, see [`docs/architecture.md`](docs/architecture.md).

---

## Stop Everything

```bash
# Stop clients: Ctrl+C in each terminal

# Stop and remove the server container (data is preserved in Docker volume)
cd server && docker compose down

# Stop and remove the server + delete all stored messages
cd server && docker compose down -v
```

---

## References

- [Apache Iggy homepage](https://iggy.apache.org)
- [Official docs](https://iggy.apache.org/docs)
- [Python SDK — iggy-py](https://github.com/iggy-rs/iggy-python-client)
- [Server source code](https://github.com/apache/iggy)
```

**Step 2: Verify the README renders and all links are internally consistent**

Check the doc links point to actual files:

```bash
ls docs/iggy-concepts.md docs/architecture.md clients/requirements.txt clients/producer.py clients/consumer.py server/docker-compose.yml
```

Expected: all six files exist.

---

## Task 8: End-to-End Smoke Test

Run through the full quick-start from a clean state to confirm everything works together.

**Step 1: Stop the server and wipe data**

```bash
cd server && docker compose down -v
```

**Step 2: Start fresh**

```bash
docker compose up -d
sleep 5
curl http://localhost:8080/api/streams   # should return []
```

**Step 3: Run producer for 10 seconds, then consumer**

```bash
cd clients && source .venv/bin/activate
timeout 10 python producer.py || true
python consumer.py &
sleep 5
kill %1
```

Expected: Consumer prints at least 9–10 messages at offsets 0 through N, then exits cleanly.

**Step 4: Restart server and verify persistence**

```bash
cd server && docker compose restart
sleep 5
curl http://localhost:8080/api/streams   # demo-stream should still exist
```

Expected: `demo-stream` and `demo-topic` are listed — data survived the restart.

---

## Done

After all tasks pass, the project is complete. The directory should contain:

```
server/docker-compose.yml
clients/requirements.txt
clients/producer.py
clients/consumer.py
docs/iggy-concepts.md
docs/architecture.md
docs/plans/2026-02-18-iggy-poc-design.md
docs/plans/2026-02-18-iggy-poc-impl.md
README.md
```
