# TicketIQ

TicketIQ is a multi-tenant API that queues customer-support ticket processing so clients do not wait for slow LLM responses.

## Architecture

```mermaid
flowchart LR
    Client[Client] -->|X-API-Key| API[FastAPI]
    API -->|Check tenant limit| Limit[(Redis token bucket)]
    API -->|Save pending job| DB[(PostgreSQL)]
    API -->|Queue job| Queue[(Redis queue)]
    Queue --> Worker[arq worker]
    Worker --> Gemini[Gemini API]
    Worker -->|Save result, status, and token usage| DB
    Client -->|Poll job ID| API
```

## Current behavior

- API keys are generated during registration and stored as SHA-256 hashes.
- Every job query is scoped to the authenticated user.
- `POST /jobs` stores a pending job, queues it, and immediately returns its ID.
- The worker processes jobs asynchronously and retries failed LLM calls three times with exponential backoff.
- A Redis token bucket limits each tenant independently and returns HTTP 429 when its bucket is empty.
- Structured logs record request duration, request IDs, job state changes, and token usage.

## Endpoints

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/register` | Register an email and receive an API key |
| `POST` | `/jobs` | Create and queue a ticket-processing job |
| `GET` | `/jobs/{job_id}` | Poll a tenant-owned job for its status and result |
| `GET` | `/health` | Check PostgreSQL and Redis connectivity |
| `GET` | `/metrics` | Read the authenticated tenant's daily requests, jobs, latency, tokens, and queue depth |

Protected endpoints require the `X-API-Key` header. Interactive API documentation is available at `/docs` while the service is running.

## Run locally

```bash
docker compose up --build
```

This starts FastAPI, one arq worker, PostgreSQL, and Redis. Docker uses the local stub provider by default so testing does not consume Gemini quota. To run against Gemini outside Docker, copy `.env.example` to `.env`, add `GENAI_API_KEY`, and keep `LLM_PROVIDER=gemini`.

## Load test

```bash
python -m pip install -r requirements-dev.txt
locust --headless --users 20 --spawn-rate 5 --run-time 30s --host http://localhost:8000
```

Every simulated user receives a separate API key. This checks that concurrent tenants can queue and poll jobs independently while excess requests from one tenant receive HTTP 429 instead of reaching the worker. Benchmark numbers will be added only after a reproducible run.

## Why these choices

**Async FastAPI:** Postgres, Redis, and Gemini calls spend most of their time waiting on network I/O. Async code lets the server work on another request during those waits.

**Redis and arq:** The queue separates quick request acceptance from slower LLM processing. It also gives retries and background workers a clear place in the architecture.

**Token bucket:** Each tenant stores only its token balance and last refill time. The Redis Lua script updates both values atomically and allows short bursts up to the bucket capacity.

## Observability

Each response includes an `X-Request-ID` that also appears in the structured request log. The worker logs when a job starts, completes, or exhausts its retries. Apply `migrations/001_observability.sql` before running this version so completed jobs can store their token counts.

## Project status

Layers 1-6 and the Locust workload are implemented. The remaining closeout step is to run the Docker stack, record the load-test results, and update this README with measured latency and throughput.
