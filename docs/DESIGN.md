# Design notes and benchmarks

Detail behind the [README](../README.md): why each piece is built the way it is, and how the system performed under load.

## Design decisions

| Problem | Approach | Why |
| --- | --- | --- |
| Writing the DB and Kafka atomically | **Transactional outbox** | Publishing inside the request can lose messages (Kafka down) or send ghosts (DB rolls back). The outbox row commits with the job, and the relay retries until Kafka has it. |
| Delayed jobs and retry backoff | Wait in **Postgres**, send to Kafka only when due | Kafka has no per-message delay. Retry topics with fixed delays are rigid. A partial index on `run_at` makes the poll cheap. |
| Several schedulers, no leader | `SELECT … FOR UPDATE SKIP LOCKED` | Every instance can poll and relay at once. Each one locks a different batch, so there's no election and no single point of failure. |
| Two workers running the same job | Conditional `UPDATE … WHERE status='QUEUED' AND attempts=:n` | Only one `UPDATE` can match. A late or duplicate message expects a different attempt number and matches nothing. |
| Crashed or hung workers | **Leases** with a heartbeat, plus a reaper | A live worker renews its lease every third of its length. One that stops is presumed dead. If it does come back, its final write is fenced off because it checks `locked_by`. |
| Duplicate HTTP requests | `Idempotency-Key` with Redis in front of a **partial unique index** | Redis answers repeats quickly. Postgres is the guarantee, even if Redis is down or two requests race. Reusing a key with a different body returns `409`. |
| Kafka redelivery (at-least-once) | `processed_messages`, written in the claim transaction | Redelivered message ids are skipped. Combined with the claim fencing, a job's handler effectively runs once per attempt. |
| Noisy tenants | **Token bucket** per tenant in Redis, as an atomic Lua script | It's shared by all API instances and uses Redis's clock, so instance clock skew doesn't matter. Bursts are allowed up to capacity. |
| Fragile downstream services | **Distributed semaphore** per job type (Redis sorted set) | It caps, say, `send-email` at 5 concurrent jobs across the whole cluster. Permits expire, so a crashed worker can't leak one. Deferred jobs don't spend an attempt. |
| Urgent work stuck behind bulk work | One topic and one consumer pool **per priority** | A backlog on `jobs.ready.low` never delays `jobs.ready.high`. |
| Redis outage | Rate limiting and concurrency caps **fail open**; idempotency falls back to Postgres | Redis is an optimization here. Losing it shouldn't stop jobs from being accepted or run. |

**Delivery guarantee.** Jobs are never lost once `POST /jobs` returns, and a job never runs twice at the same time. A job can run more than once only if a worker finishes the work and then crashes before recording success. Handlers should therefore be idempotent, which is the honest guarantee every at-least-once system gives.

## Benchmarks

Run with [k6](https://k6.io) (`loadtest/submit-jobs.js`). End-to-end latency is computed from the database afterwards (`loadtest/report.sql`), measured from job creation to completion. The job type is `noop`, so the numbers show the scheduler's own overhead.

Environment: one 4 vCPU / 16 GB cloud VM running everything (Postgres, Kafka, Redis, the scheduler instances and k6), with `worker.concurrency.normal=6` and `outbox.poll-interval-ms=50`.

| Setup | Submit rate | Jobs completed | Submit p95 | End-to-end p50 | p95 | p99 |
| --- | --- | --- | --- | --- | --- | --- |
| 1 instance | 200 /s for 60 s | 12,001 / 12,001 | 8 ms | 39 ms | 60 ms | 67 ms |
| 1 instance | 500 /s for 60 s | 29,993 / 29,993 | 29 ms | 62 ms | 867 ms | 1,299 ms |
| 2 instances | 500 /s for 60 s | 29,964 / 29,964 | 32 ms | 51 ms | 358 ms | 738 ms |
| 2 instances | 800 /s for 60 s | Saturated: about 570 jobs/s completed while the backlog grew | 184 ms | n/a | n/a | n/a |

All runs had 0 failed requests. With two instances the work split evenly, 15,038 and 14,926 jobs. On this single shared machine the ceiling is about 570 jobs/s, because the scheduler shares four cores with Postgres, Kafka and k6. On separate machines, more instances and partitions would push it further.

To run it yourself:

```bash
k6 run -e RATE=200 -e DURATION=60s -e RUN_ID=my-run loadtest/submit-jobs.js
docker compose exec -T postgres psql -U scheduler -v run_id="'my-run'" -f - < loadtest/report.sql
```

