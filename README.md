# Snaq

__This project is still working-on-progress__

Snaq is a high-performance in-memory message broker written in Elixir.

## Quick Start

### 1. Run Server
```bash
mix run --no-halt
# With logging: ELIXIR_LOG=info mix run --no-halt
```

### 2. Basic Usage
```bash
# Produce message to a topic
./snaq-cli produce --topic jobs --data "task1"

# Consume from queue (non-blocking)
./snaq-cli consume --queue jobs

# Consume with timeout (blocking, milliseconds)
./snaq-cli consume --queue jobs --timeout 5000
```

### 3. Fan-out to Multiple Queues
```bash
# Create resources
./snaq-cli create-topic --topic events
./snaq-cli create-queue --queue logger
./snaq-cli create-queue --queue metrics

# Bind queues to topic
./snaq-cli bind-queue --topic events --queue logger
./snaq-cli bind-queue --topic events --queue metrics

# Produce once, consumed by both queues
./snaq-cli produce --topic events --data "user_login"
./snaq-cli consume --queue logger
./snaq-cli consume --queue metrics
```

## Features (TODO)

- **Auto-create**: Topics and queues created automatically on first use
- **Blocking Consume**: Wait for messages with configurable timeout (milliseconds)
- **Fan-out Routing**: One topic can broadcast to multiple queues
- **Persistence**: Write-ahead log (WAL) for messages, JSON for metadata
- **Clustering**: Rendezvous hashing for distributed leadership
- **Graceful Shutdown**: SIGINT/SIGTERM handling with state saving

## Architecture

```
Producer → Topic (routing) → Queue 1 (storage) → Consumer 1
                           → Queue 2 (storage) → Consumer 2
```

- **Topic**: Routing address (no storage, distributes to bound queues)
- **Queue**: In-memory buffer with WAL persistence
- **Binding**: Many-to-many relationship between topics and queues

