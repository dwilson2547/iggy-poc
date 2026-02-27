# Iggy POC Design

**Date:** 2026-02-18
**Status:** Approved

## Goal

Build a sample Apache Iggy project demonstrating simple pub/sub messaging with a Docker-hosted server and Python producer/consumer clients, accompanied by comprehensive documentation for developers unfamiliar with Iggy.

## Architecture

```
iggy-poc/
├── README.md                    # Quick-start + demo walkthrough
├── server/
│   └── docker-compose.yml       # Iggy server (ports 8080/3000/8090)
├── clients/
│   ├── requirements.txt         # iggy-py, loguru
│   ├── producer.py              # async producer, 1 msg/sec indefinitely
│   └── consumer.py              # async consumer, polls and prints messages
└── docs/
    ├── iggy-concepts.md         # Full concept reference
    └── architecture.md          # Iggy server internals + SDK integration
```

**Data flow:** `producer.py` → TCP:8090 → Iggy server (persists to disk) → TCP:8090 → `consumer.py`

### Iggy Server Ports
| Port | Protocol | Purpose |
|------|----------|---------|
| 8090 | TCP binary | Python SDK transport |
| 8080 | HTTP REST | curl-able inspection API |
| 3000 | WebSocket | WS clients |

## Components

### server/docker-compose.yml
- Image: `iggyrs/iggy:latest`
- Named volume `iggy-data` for persistence across restarts
- All three ports mapped
- Default credentials: `iggy / iggy` via `IGGY_ROOT_PASSWORD=iggy`

### clients/producer.py
- Connects via `IggyClient` on TCP (`iggy://iggy:iggy@localhost:8090`)
- Idempotently creates stream `demo-stream` and topic `demo-topic` (1 partition)
- Sends 1 message/sec in an infinite loop
- Message format: `{"id": N, "text": "hello from producer", "ts": "<ISO timestamp>"}`
- Logs each send with loguru

### clients/consumer.py
- Connects same way as producer
- Uses `PollingStrategy.Next()` + `auto_commit=True` (server-side offset tracking)
- Polls every 500ms, prints decoded payload + offset for each message
- Handles "no messages yet" gracefully

### clients/requirements.txt
```
iggy-py>=0.4.0
loguru>=0.7.0
```

## Documentation Plan

### docs/iggy-concepts.md
Comprehensive concept reference covering:
1. What Iggy is and why it exists (vs Kafka/NATS/RabbitMQ)
2. Streams — top-level namespace
3. Topics — ordered message log channel
4. Partitions — parallelism subdivisions with per-partition offsets
5. Offsets & polling strategies (Next, Offset(N), First, Last)
6. Consumer groups — server-side shared consumption tracking
7. Transport protocols — TCP binary vs HTTP vs QUIC/WS trade-offs
8. Persistence model — segment files, retention
9. Key differences from Kafka

### docs/architecture.md
- Iggy server internals (thread-per-core, io_uring, segment storage)
- Python SDK connection flow (TCP binary framing, auth handshake)
- Port map

### README.md
- Prerequisites (Docker, Python 3.10+)
- 3-step quick start
- Links to docs/

## Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Transport protocol | TCP binary | Native Iggy protocol, lowest latency, uses iggy-py SDK |
| Messaging pattern | Simple pub/sub | Best for demonstrating core Iggy mechanics |
| Project structure | Flat & minimal | Keeps focus on Iggy, not Python packaging |
| Documentation depth | Comprehensive | Full concept reference for Iggy newcomers |
