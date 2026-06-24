# Usage Period Billing Migration: Architecture Redesign, Scaling Strategy, and Zero-Downtime Migration
## 1. Architectural Redesign

The current architecture relies on a Billing API periodically scraping archived usage periods from PostgreSQL and transforming them into billable events for Metronome. To improve scalability, resilience, and billing latency, I redesigned the ingestion pipeline around Kafka and the Transactional Outbox Pattern.

Instead of directly polling usage records, the Usage Service becomes the sole owner of usage session state and billing chunk generation. A long-running sandbox is represented by a single active usage_session record containing a last_billed_at timestamp. A scheduler periodically identifies sessions that require billing and generates billing chunks based on the configured billing interval (e.g. one hour).

When a billing chunk is generated, the Usage Service updates last_billed_at and inserts an event into an outbox_events table within the same database transaction. This guarantees that billing state changes and event creation remain atomic.

An Outbox Publisher service continuously polls unpublished events from the outbox table, performs idempotency checks against a processed_outbox_events table keyed by a unique event_id, and publishes them to a Kafka topic named usage-billing-chunks. The published marker and the processed record are written in the same transaction, and any event already recorded as processed is marked published in that transaction so it is never re-leased. Once Kafka acknowledges successful publication, the outbox event is marked as published.

The Billing Processor consumes events from Kafka, performs an idempotency check, transforms billing chunks into Metronome usage events, and submits them to Metronome. Successful processing is recorded by inserting a row into processed_billing_events (unique on event_id) within the same transaction that completes the work, so a duplicate delivery is rejected by the unique constraint rather than re-charged. Because the billing-processed event is emitted before the transaction commits, that topic is delivered at-least-once; Analytics consumers therefore also deduplicate by event_id when exporting to ClickHouse, and can subscribe independently without affecting billing throughput.

This architecture decouples usage tracking from billing ingestion, removes direct database scraping, and provides durable event storage in case downstream services become unavailable. A separate migration-service handles one-shot schema initialization and table/index creation prior to the services starting.

## 2. Scaling Strategy

The current implementation creates a new usage period every 24 hours by closing and reopening records. Reducing the billing interval to one hour would significantly increase database writes and archival operations. Instead of creating a new database row for every billing interval, I  rewrote it to maintain a single active usage_session record per sandbox and track billing progress using a last_billed_at timestamp.

The Usage Service scheduler only queries sessions where:

`"status" = "SESSION_ACTIVE" AND last_billed_at <= now() - billing_interval`

Newly created sessions are seeded with last_billed_at set to their start_at rather than NULL, because `NULL < now() - billing_interval` never evaluates to true and would otherwise hide fresh sessions from the scheduler entirely.

When a session becomes eligible for billing, the scheduler updates last_billed_at, increments a billing sequence number, and writes a billing event to the outbox table in a single transaction.

To minimize database contention, separate indexes are created on last_billed_at, end_at, and status, and sessions are processed in batches using:

`FOR UPDATE SKIP LOCKED`

This allows multiple scheduler instances to run concurrently while ensuring that a session is only processed by a single worker.

For Kafka partitioning, I used sandbox_id as the message key. This guarantees ordering for billing events belonging to the same sandbox while evenly distributing load across partitions. Downstream, the billing-processed topic is instead keyed by session_id, preserving per-session ordering for analytics while the upstream chunk topic preserves per-sandbox ordering. I would initially provision 48 partitions before jumping to 96 after hitting 1 billion monthly events, providing sufficient headroom for future growth without frequent repartitioning.

The Billing Processor would run as a horizontally scalable Kafka consumer group named billing-processors. Kafka automatically distributes partitions among available consumers, allowing throughput to scale linearly by adding more processor replicas. Each processor performs idempotency checks, sends events to Metronome, records successful processing, and commits offsets only after successful completion.

At the projected scale at the end of the year of approximately 500 million billing events per month (roughly 200 events per second on average), Kafka is unlikely to be the primary bottleneck. The more significant scaling concerns are scheduler efficiency, database write amplification, downstream Metronome throughput, and idempotent event processing.

## 3. Durability and Failure Handling

To guarantee that billing events are never lost, the system uses the Transactional Outbox Pattern. Billing state updates and outbox event creation occur within the same database transaction. If Kafka, the Outbox Publisher, or downstream services become unavailable, billing events remain safely persisted in PostgreSQL until delivery succeeds.

The Outbox Publisher provides at-least-once delivery semantics when publishing events to Kafka. Duplicate Kafka messages are expected and handled through idempotent processing at every stage. The primary deduplication key end-to-end is the event_id, enforced by unique constraints on processed_outbox_events (publisher) and processed_billing_events (consumer); an already-seen event_id is skipped before any external side effect occurs.

As a final backstop at the external sink, the Billing Processor also assigns every billing chunk a deterministic transaction identifier:

session_id:sequence

This identifier is submitted to Metronome so that even if the same chunk reaches it more than once, Metronome's idempotent ingestion collapses the duplicates.

Kafka offsets are committed only after successful processing. If a consumer crashes after processing an event but before committing its offset, Kafka will redeliver the message. The processor detects that the event has already been processed and safely skips duplicate work.

To exercise this guarantee, the pipeline can periodically replay already-published outbox events, injecting deliberate duplicate deliveries. The Billing Processor detects the repeated event_id, skips the redundant work, and downstream billing and analytics counts remain unchanged, demonstrating that duplicate events cannot cause double charges.

This design ensures that outages affecting Kafka, Metronome, ClickHouse, or individual processor instances cannot result in lost billing events or duplicate customer charges.

## 4. Zero-Downtime Migration Strategy

I would use a dual-write, dual-read migration strategy.

First, I would introduce the new Outbox + Kafka pipeline while keeping the existing PostgreSQL scraper fully operational. The Usage Service continues generating billing chunks exactly as today, but additionally writes billing events into the outbox table within the same transaction. An Outbox Publisher forwards these events to Kafka.

At this stage, the Kafka-based Billing Processor operates in shadow mode. It consumes and validates billing events, verifies payload correctness, and monitors throughput without sending data to Metronome.

Once the Kafka pipeline has been validated, Metronome ingestion is enabled in the new Billing Processor while the legacy scraper remains active as a fallback. To prevent double billing during this transition period, every billing chunk receives a deterministic transaction identifier such as session_id:sequence or usage_period_id. Both the legacy and Kafka-based pipelines use the same identifier when submitting events to Metronome, ensuring duplicate submissions are safely deduplicated.

The final cutover is performed gradually by disabling Metronome writes in the legacy scraper while continuing to monitor Kafka consumer lag, billing event counts, reconciliation metrics, and downstream processing health. After a verification period confirms that all billing events are flowing correctly through Kafka and event counts match the legacy system, the scraper can be safely retired.

This migration strategy provides zero downtime, guarantees that no billing events are lost, and prevents customers from being charged twice for the same usage period.

## 5. Scaling Model for Outbox and Billing Pipeline (Partitions, Consumers, and Future Bottlenecks)

The system uses Kafka as the primary transport layer between the Outbox service and downstream Billing services. Scaling is achieved primarily through partitioning and consumer parallelism rather than increasing per-instance workload.

### Baseline Kafka Topology

The system operates with:

- 48–96 Kafka partitions per high-throughput topic (e.g., `usage-billing-chunks`, `billing-processed`)
- Consumer groups for independent processing stages:
  - Outbox Publisher group
  - Billing Processor group
  - Analytics/Export group

Each consumer group scales independently, and each partition is consumed by exactly one consumer per group.

This creates an upper bound on parallelism per service:

Max active consumers per group = number of partitions (48–96)

### Consumer Scaling Model

#### Outbox Service (Publisher)

Role:
- Reads from PostgreSQL outbox table
- Publishes events to Kafka

Scaling behavior:

- Typically CPU + DB + network bound
- Scales horizontally via multiple service replicas
- Each instance runs ~16–32 workers internally
- Kafka is not the bottleneck here; Postgres is

Effective throughput is driven by:
- Postgres SKIP LOCKED leasing performance
- Kafka producer batching efficiency
- Network throughput

Recommended scaling:
- 4–20 instances depending on load
- 32 workers per instance as baseline

#### Billing Service (Consumer)

Role:
- Consumes billing events
- Performs idempotency checks
- Calls external systems (e.g., billing engines)
- Writes results to database / emits downstream events

Scaling behavior:
- Strongly I/O bound (external API + DB writes)
- Partition-limited parallelism (max 48–96 concurrent active consumers)
- Additional concurrency must be achieved inside each consumer (worker pools or async pipelines)

Recommended scaling:
- 1 consumer group with 48–96 active consumers max
- Each consumer may use internal concurrency (5–20 goroutines depending on downstream limits)

### Throughput Scenarios

The following estimates assume:
- Efficient Kafka batching
- Stable Postgres performance
- No major retry storms
- Average event size remains small to moderate

### 500 Million Events / Month
#### ~192 events/sec average

System capability:
- Easily handled by 48 partitions
- ~5–10 consumer instances per service layer

Bottlenecks:
- None significant
- Postgres outbox write throughput may become more important than Kafka

### 5 Billion Events / Month
#### ~1,900 events/sec average

System behavior:
- Requires full partition utilization (48–96)
- Outbox service becomes DB-heavy
- Billing consumers begin to hit external API throughput limits

Bottlenecks:
- Postgres write amplification (outbox table growth)
- Kafka producer backpressure during spikes 
- External billing API rate limits

Mitigation:
- Increase Kafka partitions toward 96
- Introduce Kafka producer batching optimizations
- Partition outbox table (by time or tenant)
- Add caching layer for idempotency checks (Redis optional)

### 50 Billion Events / Month
#### ~19,000 events/sec average

System constraints:
- Kafka remains viable with 96 partitions but is near saturation per partition
- Postgres outbox becomes a primary bottleneck
- Billing service must decouple synchronous external calls

Bottlenecks:
- Postgres WAL and index pressure
- Consumer lag accumulation during peaks
- External API saturation

Mitigation strategies:
- Move from single outbox table → partitioned tables (by time/tenant)
- Introduce Redis-based idempotency caching layer (reduce DB reads)
- Split Billing pipeline into stages (ingest → enrich → charge)
- Add async buffering layer between billing steps

### 200 Billion Events / Month
#### ~77,000 events/sec average

At this scale, Kafka + Postgres alone are insufficient as a single tightly coupled pipeline.

Bottlenecks become structural:
- Postgres cannot sustain global outbox writes at this rate
- Kafka partition ceiling (even 96 partitions) limits parallelism per topic
- Consumer groups become coordination-limited
- Billing external systems become primary throughput limiter

Required architectural evolution:
1. Remove Postgres as a real-time queue
   - Replace outbox table with:
   - Kafka-first ingestion
   - or dual-write with async reconciliation
2. Introduce Redis / streaming buffer layer
   - Redis Streams or similar for short-lived buffering
   - absorbs burst traffic before Kafka
3. Decouple billing pipeline stages 
   - Split into independent services:
     - Event ingestion service
     - Billing calculation service
     - Charging service
     - Settlement service
   - Each stage scales independently via its own Kafka topic and partitioning strategy.
   
4. Multi-topic sharding strategy
   - Instead of one hot topic:
     - shard by tenant / region / time window
     - reduces partition contention

5. Move idempotency away from Postgres
   - Redis or dedicated key-value store for high-throughput dedupe
   - periodic reconciliation back to durable storage