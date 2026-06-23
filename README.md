# Usage Period Migration: Billing & Analytics System

This project is a distributed microservices-based billing and usage-tracking system designed to process customer usage sessions, generate billing events, and export analytics. It handles the complete lifecycle from session creation through billing chunk generation, using an event-driven architecture with Kafka for messaging and PostgreSQL for state management.

The system consists of 4 Go services: 
- #### Usage Service 
  - manages the lifecycle of usage sessions (create, finish, track) - "Ingestor" that simulates the Usage Service producing events 
- #### Billing Processor 
  - consumes billing events from Kafka and transforms them into metronome billing payloads -  "Processor" that simulates the Billing API consuming events 
- #### Outbox Publisher 
  - implements the transactional outbox pattern to reliably publish billing chunks to Kafka, using a distributed worker pool with leasing for concurrent processing 
- #### Analytics Exporter 
  - consumes processed billing events and exports them to analytics platforms (with Clickhouse simulation in this implementation)

The architecture emphasizes reliability and eventual consistency through the outbox pattern, all critical state changes are first written to a local outbox table within a transaction, then published asynchronously to Kafka. This guarantees no events are lost even if services crash. Idempotency tracking ensures duplicate events are safely handled. The system uses structured JSON logging throughout for production observability.

All services are containerized with Docker and orchestrated via docker-compose for local development. The infrastructure includes PostgreSQL 18.4 with auto-incrementing IDs, Kafka 3.8 with Zookeeper for reliable message queuing, and comprehensive CI/CD via CircleCI with parallel build and test jobs. Database initialization is handled through schema migration functions that create tables with appropriate indexes on critical query paths.

The codebase follows clean architecture principles with interface-based repositories for testability, comprehensive table-driven unit tests, and proper error handling throughout. Dependencies are managed via Go modules, and all services compile to small Alpine-based Docker images suitable for production deployment.