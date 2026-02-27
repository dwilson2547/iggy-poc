# Apache Iggy — Architecture Overview

This document describes how the Iggy server works internally and how the Python SDK connects to it. It complements [iggy-concepts.md](iggy-concepts.md), which covers the logical model.

---

## Server Internals

```
┌──────────────────────────────────────────────────────────────────────┐
│                          Iggy Server Process                         │
│                                                                      │
│  ┌──────────────────────────────────────────────────────────────┐    │
│  │                  Transport Layer (per-thread)                │    │
│  │  ┌──────────────┐  ┌─────────────────────────────────────┐   │    │
│  │  │  TCP binary  │  │  HTTP REST / QUIC / WebSocket       │   │    │
│  │  │   :8090      │  │              :3000                  │   │    │
│  │  └──────┬───────┘  └──────────────────┬──────────────────┘   │    │
│  └─────────┼───────────────────────────── ┼─────────────────────┘    │
│            │                              │                          │
│  ┌─────────▼──────────────────────────── ▼──────────────────────┐    │
│  │                     Command Dispatcher                       │    │
│  │  (deserialise → validate → route to stream/topic handler)    │    │
│  └─────────────────────────────┬────────────────────────────────┘    │
│                                │                                     │
│  ┌─────────────────────────────▼──────────────────────────────────┐  │
│  │                     Storage Engine                             │  │
│  │                                                                │  │
│  │  Stream A            Stream B                                  │  │
│  │  └── Topic X         └── Topic Y                               │  │
│  │      ├── Partition 1     ├── Partition 1                       │  │
│  │      │   ├── seg-0.log  │   ├── seg-0.log                      │  │
│  │      │   └── seg-1.log  │   └── seg-1.log                      │  │
│  │      └── Partition 2    └── Partition 2                        │  │
│  └─────────────────────────────────────────────────────────────── ┘  │
└──────────────────────────────────────────────────────────────────────┘
         │ disk I/O via io_uring (Linux async I/O, zero-copy)
         ▼
    /iggy/data/  (bind-mounted Docker volume)
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
        │  1. IggyClient()              ← construct client (default: localhost:8090 TCP)
        │  2. await client.connect()    ← open TCP socket, complete handshake
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

Iggy uses username/password authentication. The default credentials (`iggy` / `iggy`) are set via the `IGGY_ROOT_USERNAME` and `IGGY_ROOT_PASSWORD` environment variables in Docker. The Python TCP SDK authenticates via `client.login_user()`. The HTTP API uses a JWT bearer token obtained from `POST /users/login`.

### Message Flow: Produce

```
producer.py
  └── client.send_messages(stream, topic, partitioning=1, messages=[SendMessage(payload)])
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
  └── client.poll_messages(stream, topic, partition_id, PollingStrategy.Next(), count=10, auto_commit=True)
        │
        │  Binary frame: POLL_MESSAGES command
        │  Server looks up consumer's stored offset, reads `count` messages
        │  If auto_commit=True: server advances stored offset after read
        ▼
  Returns: list[ReceiveMessage]
    └── message.offset()  → absolute position in partition
    └── message.payload() → message payload as str (or bytes on some platforms)
```

---

## Port Reference

| Port | Protocol | Purpose | Example |
|------|----------|---------|---------|
| 8090 | TCP binary | Python SDK, Go/Rust/C# SDKs | `iggy-py`, Rust client, Go client |
| 3000 | HTTP REST | curl inspection, management API | `curl -H "Authorization: Bearer $TOKEN" http://localhost:3000/streams` |
| 3000 | QUIC | Low-latency UDP transport | Requires TLS configuration |
| 3000 | WebSocket | Browser or WS clients | `ws://localhost:3000` |

### curl Quick Reference

```bash
# Login and get token
TOKEN=$(curl -s -X POST http://localhost:3000/users/login \
  -H 'Content-Type: application/json' \
  -d '{"username":"iggy","password":"iggy"}' \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")

# List streams
curl -s -H "Authorization: Bearer $TOKEN" http://localhost:3000/streams

# Get topic info
curl -s -H "Authorization: Bearer $TOKEN" \
  http://localhost:3000/streams/demo-stream/topics/demo-topic
```

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
