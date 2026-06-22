# Outbox Publisher Service

This service implements the Transactional Outbox Pattern to reliably publish billing events from the database to Kafka.

## Overview

The Outbox Publisher Service:
1. Polls the `outbox_events` table for unpublished events
2. Parses the event payload into `BillingChunkCreated` messages
3. Publishes them to the Kafka topic `usage-billing-chunks`
4. Marks the events as published in the database

## Architecture

```
┌─────────────┐      ┌──────────────────┐      ┌───────────┐
│  Database   │─────▶│ Outbox Publisher │─────▶│   Kafka   │
│ (Outbox)    │      │    Service       │      │  (Topic)  │
└─────────────┘      └──────────────────┘      └───────────┘
     ▲                                                │
     │                                                │
     └────────────────────────────────────────────────┘
            Updates published_at after success
```

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DB_HOST` | PostgreSQL host | `localhost` |
| `DB_PORT` | PostgreSQL port | `5432` |
| `DB_USER` | PostgreSQL user | `postgres` |
| `DB_PASSWORD` | PostgreSQL password | `password` |
| `DB_NAME` | PostgreSQL database name | `usage_db` |
| `DB_SSL_MODE` | PostgreSQL SSL mode | `disable` |
| `KAFKA_BROKERS` | Kafka broker addresses (comma-separated) | `localhost:9092` |
| `KAFKA_CONSUMER_GROUP` | Kafka consumer group ID | `usage-billing-processor` |
| `KAFKA_MAX_ATTEMPTS` | Maximum Kafka retry attempts | `3` |
| `POLLING_INTERVAL_SECONDS` | How often to poll for new events | `5` |
| `BATCH_SIZE` | Number of events to process per batch | `100` |

### Example .env file

```bash
# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=your_password
DB_NAME=usage_db
DB_SSL_MODE=disable

# Kafka Configuration
KAFKA_BROKERS=localhost:9092,localhost:9093,localhost:9094
KAFKA_CONSUMER_GROUP=usage-outbox-processor
KAFKA_MAX_ATTEMPTS=3

# Polling Configuration
POLLING_INTERVAL_SECONDS=5
BATCH_SIZE=100
```

## Running the Service

### Local Development

```bash
# Set environment variables
export DB_HOST=localhost
export DB_PORT=5432
export DB_USER=postgres
export DB_PASSWORD=password
export KAFKA_BROKERS=localhost:9092

# Run the service
cd outbox-publisher/cmd
go run main.go
```

### Using .env file

```bash
# Load environment variables from .env file
export $(cat .env | xargs)

# Run the service
go run outbox-publisher/cmd/main.go
```

### Build and Run

```bash
# Build
go build -o bin/outbox-publisher outbox-publisher/cmd/main.go

# Run
./bin/outbox-publisher
```

## Service Behavior

### Polling Cycle

1. Service polls every `POLLING_INTERVAL_SECONDS` seconds
2. Fetches up to `BATCH_SIZE` unpublished events
3. For each event:
   - Validates the event type is `billing_chunk_created`
   - Unmarshals the JSON payload
   - Validates the billing chunk data
   - Publishes to Kafka topic `usage-billing-chunks`
   - Marks event as published with `published_at` timestamp
4. Logs statistics and any errors

### Error Handling

- **Parse Errors**: Event is skipped and logged
- **Validation Errors**: Event is skipped and logged
- **Kafka Errors**: Event retry count is incremented, error is logged
- **Database Errors**: Logged and retried on next polling cycle

### Retry Logic

- Failed events are automatically retried on the next polling cycle
- Retry count is tracked in the `retry_count` column
- Last error is stored in the `last_error` column
- Use `ProcessUnpublishedEventsWithRetry()` to implement max retry limits

## Monitoring

### Logs

The service logs:
- Startup and shutdown events
- Database and Kafka connection status
- Number of events processed per cycle
- Individual event processing success/failure
- Final statistics on shutdown

### Statistics

Access runtime statistics via:
```go
stats := publisherService.GetStats()
fmt.Printf("Total Processed: %d\n", stats.TotalProcessed)
fmt.Printf("Total Failed: %d\n", stats.TotalFailed)
fmt.Printf("Last Processed: %s\n", stats.LastProcessed)
```

## Kafka Topic

### Topic Name
`usage-billing-chunks`

### Partition Key
Messages are partitioned by `sandbox_id` to ensure ordering per sandbox.

### Message Format
```json
{
  "event_id": "uuid",
  "session_id": "session-123",
  "sandbox_id": "sandbox-456",
  "organization_id": "org-789",
  "sequence": 5,
  "from": "2026-06-21T10:00:00Z",
  "to": "2026-06-21T11:00:00Z",
  "cpu": 2.0,
  "gpu": 1.0,
  "ram_gb": 8.0,
  "disk_gb": 50.0,
  "region": "us-east-1",
  "region_type": "cloud",
  "sandbox_class": "container"
}
```

## Graceful Shutdown

The service handles `SIGINT` and `SIGTERM` signals:
1. Stops polling for new events
2. Completes current processing cycle
3. Closes Kafka connections
4. Closes database connections
5. Prints final statistics

## Performance Optimization

### Batch Publishing

For better throughput, use batch publishing:
```go
count, err := publisherService.BatchPublish(ctx, 1000)
```

This publishes multiple events to Kafka in a single batch operation.

### Tuning Parameters

- **Increase `BATCH_SIZE`**: Process more events per cycle
- **Decrease `POLLING_INTERVAL_SECONDS`**: Lower latency
- **Adjust `KAFKA_MAX_ATTEMPTS`**: Balance between reliability and speed

## Troubleshooting

### Events Not Being Published

1. Check database connection: `SELECT 1` should work
2. Verify unpublished events exist: `SELECT COUNT(*) FROM outbox_events WHERE published_at IS NULL`
3. Check Kafka connectivity: Ensure brokers are reachable
4. Review logs for specific error messages

### High Retry Counts

- Network issues with Kafka cluster
- Invalid event payloads
- Kafka topic not created or not accessible

### Memory Issues

- Reduce `BATCH_SIZE`
- Increase `POLLING_INTERVAL_SECONDS`
- Add memory limits in deployment

## Database Schema

The service expects the following table:

```sql
CREATE TABLE outbox_events (
    id UUID PRIMARY KEY,
    aggregate_type VARCHAR(255) NOT NULL,
    aggregate_id VARCHAR(255) NOT NULL,
    event_type VARCHAR(255) NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMP NOT NULL,
    published_at TIMESTAMP,
    retry_count INT DEFAULT 0,
    last_error TEXT
);

CREATE INDEX idx_outbox_unpublished ON outbox_events(published_at) 
WHERE published_at IS NULL;
```

