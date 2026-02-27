# Apache Iggy POC

A minimal working example of [Apache Iggy](https://iggy.apache.org) — a hyper-efficient persistent message streaming platform written in Rust.

This POC demonstrates **simple pub/sub messaging** using:

- An Iggy server running in Docker
- Client implementations in **Python**, **Java**, **Rust**, and **TypeScript**
  - Python async producer and consumer
  - Java blocking producer and consumer (Maven)
  - Rust async producer and consumer (Cargo)
  - TypeScript async producer and consumer (npm)

---

## Prerequisites

| Tool | Version | Notes |
|------|---------|-------|
| Docker + Docker Compose | Any recent | For the Iggy server |
| Python | 3.10+ | For the Python clients |
| Java + Maven | 17+ / 3.6+ | For the Java clients |
| Rust + Cargo | 1.70+ | For the Rust clients |
| Node.js + npm | 18+ | For the TypeScript clients |

---

## Quick Start

### 1. Start the Iggy server

```bash
cd server
docker compose up -d
```

Verify it's running (login first to get a token, then list streams):

```bash
TOKEN=$(curl -s -X POST http://localhost:3000/users/login \
  -H 'Content-Type: application/json' \
  -d '{"username":"iggy","password":"iggy"}' \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")

curl -s -H "Authorization: Bearer $TOKEN" http://localhost:3000/streams
# → []  (empty — no streams yet, but server is up)
```

### 2. Run the clients

Choose **Python**, **Java**, **Rust**, or **TypeScript** (or run multiple at the same time!):

#### Option A: Python clients

```bash
cd clients/python
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# Terminal 1 - Producer
python producer.py

# Terminal 2 - Consumer
python consumer.py
```

#### Option B: Java clients

```bash
cd clients/java
mvn clean install

# Terminal 1 - Producer
mvn exec:java@run-producer

# Terminal 2 - Consumer  
mvn exec:java@run-consumer
```

#### Option C: Rust clients

```bash
cd clients/rust
cargo build --release

# Terminal 1 - Producer
cargo run --bin producer

# Terminal 2 - Consumer
cargo run --bin consumer
```

#### Option D: TypeScript clients

```bash
cd clients/typescript
npm install

# Terminal 1 - Producer
npm run producer

# Terminal 2 - Consumer
npm run consumer
```

You'll see messages flowing from producer to consumer:

```text
# Producer
INFO  Sent message #1: {"id": 1, "text": "hello from producer", "ts": "..."}

# Consumer
INFO  [offset=0] {"id": 1, "text": "hello from producer", "ts": "..."}
```

---

## Project Layout

```text
iggy-poc/
├── README.md
├── server/
│   └── docker-compose.yml           # Iggy server (TCP:8090, HTTP+WS:3000)
├── clients/
│   ├── python/
│   │   ├── producer.py              # Python async producer
│   │   ├── consumer.py              # Python async consumer
│   │   └── requirements.txt
│   ├── java/
│   │   ├── pom.xml                  # Maven configuration
│   │   └── src/main/java/.../
│   │       ├── Producer.java        # Java blocking producer
│   │       └── Consumer.java        # Java blocking consumer
│   ├── rust/
│   │   ├── Cargo.toml               # Cargo configuration
│   │   └── src/
│   │       ├── producer.rs          # Rust async producer
│   │       └── consumer.rs          # Rust async consumer
│   └── typescript/
│       ├── package.json             # npm configuration
│       └── src/
│           ├── producer.ts          # TypeScript async producer
│           └── consumer.ts          # TypeScript async consumer
└── docs/
    ├── iggy-concepts.md             # Core concept reference
    └── architecture.md              # Server internals + SDK flow
```

---

## Inspect With curl

The HTTP REST API on port 3000 lets you inspect the server state. Authentication is required — get a token first:

```bash
# Get a token
TOKEN=$(curl -s -X POST http://localhost:3000/users/login \
  -H 'Content-Type: application/json' \
  -d '{"username":"iggy","password":"iggy"}' \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")

# List all streams
curl -s -H "Authorization: Bearer $TOKEN" http://localhost:3000/streams

# Get topic details (includes message count)
curl -s -H "Authorization: Bearer $TOKEN" \
  http://localhost:3000/streams/demo-stream/topics/demo-topic

# Get partitions (includes current offset)
curl -s -H "Authorization: Bearer $TOKEN" \
  http://localhost:3000/streams/demo-stream/topics/demo-topic/partitions
```

---

## Key Concepts

New to Iggy? Start with [`docs/iggy-concepts.md`](docs/iggy-concepts.md):

- **Streams** — top-level namespace
- **Topics** — ordered message log
- **Partitions** — parallelism unit with per-offset tracking
- **Polling strategies** — `Next()`, `Offset(n)`, `First()`, `Last()`
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
- [Rust SDK — iggy](https://crates.io/crates/iggy)
- [TypeScript SDK — @iggy.rs/sdk](https://www.npmjs.com/package/@iggy.rs/sdk)
- [Server source code](https://github.com/apache/iggy)
