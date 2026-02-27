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
| TCP binary | 8090 | Production clients — lowest latency, binary framing |
| HTTP REST | 3000 | Debugging, curl inspection, language-agnostic |
| QUIC | 3000 | Low-latency over UDP; requires TLS setup |
| WebSocket | 3000 | Browser clients, real-time dashboards |

The Python `iggy-py` SDK uses the **TCP binary** protocol. The HTTP API is useful for ad-hoc inspection without a dedicated SDK.

> **Note:** The `iggyrs/iggy:latest` Docker image binds HTTP, QUIC, and WebSocket all on port 3000. Port 8090 is exclusively for the TCP binary protocol.

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
