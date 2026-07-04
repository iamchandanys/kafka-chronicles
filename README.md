# Kafka Chronicles

## Why Kafka over SQS / Service Bus?

SQS and Service Bus are **queues** — a message is consumed, acknowledged, and gone. Kafka is a **durable, replayable log** — messages stick around for a retention period you choose (even "forever"), and multiple independent readers can each replay the full history at their own pace. That difference explains most of why Kafka persists despite being more ops-heavy.

| | Kafka | SQS / Service Bus |
|---|---|---|
| After a consumer reads a message | Still there — any other consumer (or the same one later) can re-read it | Gone (deleted/acked) |
| Multiple independent consumers on the same data | Native — each consumer group tracks its own offset, replays independently | Possible via fan-out (SNS+SQS, or Service Bus Topics), but no long-term replay of history |
| Reprocessing after a bug | Just rewind the offset and replay | Not possible — the data is gone once consumed |
| Throughput ceiling | Very high (millions/sec via partitioning) | High, but not built primarily for streaming-scale analytics workloads |
| Ecosystem | Kafka Streams, ksqlDB, Flink/Spark integration — used to build real-time pipelines, not just move messages | Just messaging — no built-in stream processing |
| Ops burden | You run it (or pay Confluent/MSK/Event Hubs) | Fully managed, zero infra to think about |
| Portability | Open source, runs anywhere — no cloud lock-in | Tied to AWS or Azure specifically |

### Kafka tends to win when
- You need to **replay** history (reprocess after a bug, backfill a new consumer, feed both real-time and batch systems from the same data).
- Many **independent teams/systems** need to consume the same event stream at different speeds without stepping on each other.
- You're building a **streaming data platform** (CDC pipelines, real-time analytics, ML feature pipelines) rather than just decoupling two services.

### SQS/Service Bus tend to win when
- You just need to decouple a couple of services with a task queue (job processing, "do this thing eventually").
- You want zero infrastructure to manage and don't care about replay.
- Volume is moderate and speed-to-market matters more than platform flexibility.

So it's less "companies still choose Kafka despite better alternatives" and more "Kafka solves a different problem" — the log/replay/fan-out model, not the point-to-point queue model.

## Cost comparison

**Kafka itself is free** — Apache License 2.0, no licensing cost no matter where you run it.

### If you self-host Kafka (e.g., on Azure/AWS VMs, AKS, EKS)
You're not paying for the software, you're paying for infrastructure:

| Cost driver | Why it costs money |
|---|---|
| **Compute** (dominant cost) | Brokers + controllers run 24/7 — you pay for that CPU/RAM uptime regardless of traffic |
| **Storage** | Kafka persists data to disk (broker logs, controller metadata) — needs persistent volumes sized for your retention period |
| **Networking** | Load balancer / public IP to reach brokers externally, plus data egress charges |
| **Container registry** | Only needed if you build a custom image — negligible (~$0.17/day) since this repo pulls `apache/kafka:latest` directly from Docker Hub |

### If you use a managed Kafka service
- **Confluent Cloud**, **AWS MSK**, **Azure Event Hubs** (Kafka-compatible tier) — you pay the vendor for hosting + support on top of underlying infra, removing the ops burden but adding a recurring bill.

### SQS / Service Bus cost model
- **Fully managed, pay-per-use** — billed per message/operation (SQS) or per messaging unit (Service Bus), no cluster to provision or size.
- No "always-on" cost — if you send nothing, you pay close to nothing.

### The actual tradeoff
At low-to-moderate volume, SQS/Service Bus is almost always **cheaper and simpler** — no cluster sizing, no disks, no ops team. Kafka's infrastructure cost only pays for itself once you actually need what it uniquely offers: replay, multi-consumer fan-out, or a streaming platform. Running Kafka just as "a queue" is usually the expensive way to get queue behavior.

## Real-world examples: when to use which

### Kafka

1. **Activity/clickstream tracking (Netflix, Spotify, Uber-style)** — every click, play, swipe, or location ping is published to one topic. That same stream is independently consumed by the recommendation engine, a fraud/anomaly detector, a real-time analytics dashboard, and a batch job into a data warehouse — all at their own pace, none deleting data for the others.
2. **Bank/fintech transaction ledger (event sourcing)** — every transaction is appended to a topic as the source of truth. Account balances are derived by replaying events, not stored as the primary fact. If a balance-calculation bug is found, fix it and replay the entire history to recompute correct balances.
3. **Change Data Capture (CDC) fan-out** — a tool like Debezium streams every database row change into Kafka. From that one topic: Elasticsearch gets updated (search), Redis gets updated (cache), Snowflake gets updated (analytics) — three unrelated systems consuming independently.

### SQS / Service Bus

1. **Order confirmation emails** — checkout service publishes "order placed" → one worker picks it up, sends a confirmation email, done. Nobody re-reads that message later. A classic one-shot task handoff.
2. **Thumbnail/video processing after upload** — a message goes on a queue → a worker pool picks up jobs, transcodes/generates thumbnails, marks done. If a worker crashes mid-job, the message reappears for another worker to retry.
3. **Decoupling from a flaky downstream API** — e.g. sending SMS/push notifications via a third-party API that occasionally times out. Drop a message on a queue with retry + dead-letter handling instead of calling it inline.

### The pattern to notice

Ask **"does more than one independent system need to read this same event, possibly replaying old history?"**
- **Yes** → Kafka (activity streams, CDC, audit logs, anything feeding multiple analytics/ML systems).
- **No, it's a single task that needs to happen reliably once** → SQS/Service Bus (emails, job processing, API decoupling).
