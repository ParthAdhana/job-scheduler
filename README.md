# Distributed Job Scheduler

A job queue where clients submit work over HTTP, jobs run now or at a scheduled time on a pool of workers, failures retry with backoff, duplicate requests are ignored, and noisy tenants are rate limited.

Built with Java 21, Spring Boot 3, PostgreSQL, Kafka and Redis.

## Status

| Day | Feature | State |
| --- | --- | --- |
| 1 | Local stack, schema, Jobs API | Done |
| 2 | Kafka dispatch (transactional outbox), workers | Planned |
| 3 | Delayed and cron jobs, retries, dead-letter topic, leases | Planned |
| 4 | Idempotency (API and consumer) | Planned |
| 5 | Rate limiting and concurrency caps | Planned |
| 6 | Integration tests and observability | Planned |
| 7 | Load test, benchmarks, CI | Planned |

## Run locally

Requirements: Java 21 and Docker.

```bash
docker compose up -d          # Postgres, Redis, Kafka, Kafka UI
./mvnw spring-boot:run        # API on http://localhost:8080
```

- API docs: http://localhost:8080/swagger-ui.html
- Health: http://localhost:8080/actuator/health
- Kafka UI: http://localhost:8081

Flyway creates the schema on startup (`src/main/resources/db/migration`).

## Run tests

```bash
./mvnw verify
```

Integration tests start a real PostgreSQL in Docker with Testcontainers, so Docker must be running.

## API

Every request is scoped to a tenant through the `X-Tenant-Id` header (defaults to `default`).

| Method | Path | Description |
| --- | --- | --- |
| `POST` | `/jobs` | Submit a job. Returns `201` with a `Location` header. |
| `GET` | `/jobs/{id}` | Fetch one job. `404` if it belongs to another tenant. |
| `GET` | `/jobs?status=PENDING&page=0&size=20` | List jobs, newest first. |
| `POST` | `/jobs/{id}/cancel` | Cancel a job that has not started. `409` otherwise. |

```bash
curl -X POST localhost:8080/jobs \
  -H 'Content-Type: application/json' \
  -H 'X-Tenant-Id: acme' \
  -d '{"type": "send-email", "payload": {"to": "a@example.com"}, "runAt": "2026-12-01T09:00:00Z", "maxAttempts": 5}'
```

Errors use RFC 7807 problem responses. Validation errors include an `errors` map of field to message.

## Job lifecycle

```
PENDING -> QUEUED -> RUNNING -> SUCCEEDED
                        |
                        v
                    RETRYING -> QUEUED ...   (until max attempts)
                        |
                        v
                      FAILED

PENDING / QUEUED / RETRYING -> CANCELLED
```

## Project layout

```
src/main/java/com/jobscheduler
  api/      REST controller, request/response records, error mapping
  job/      Job entity, status rules, repository, service
  common/   Shared config (Clock)
src/main/resources/db/migration   Flyway SQL migrations
```
