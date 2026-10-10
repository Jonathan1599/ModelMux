# ModelMux

ModelMux is a provider-agnostic LLM inference gateway built with Node.js, TypeScript, and Fastify. It authenticates clients with API keys, applies a distributed per-key token bucket, and supports synchronous chat, SSE streaming, and asynchronous inference jobs backed by RabbitMQ and Postgres.

Ollama is the primary provider. Optional OpenAI fallback uses explicit model mappings, with a separate circuit breaker for each provider.

## Architecture

```text
Client
  -> Bearer API key authentication
       -> SHA-256 key hash lookup in Postgres
       -> per-key capacity and refill policy
  -> atomic Redis token bucket
  -> Fastify schema validation
  -> POST /v1/chat
       -> opt-in exact cache (hit returns immediately)
       -> bounded process-local concurrency limiter
       -> ProviderRouter -> Ollama (primary) -> ChatResponse
                         -> OpenAI (configured fallback on transient failures)
  -> POST /v1/chat/stream
       -> same concurrency limiter and provider router (no RabbitMQ)
       -> Ollama / configured OpenAI fallback -> SSE deltas -> client
  -> POST /v1/jobs + Idempotency-Key
       -> JobQueue.enqueue() -> Postgres job + pending-publication flag
       -> 202 Accepted + job ID

Separate worker process
  -> dispatch pending jobs to RabbitMQ using publisher confirms
  -> consume job IDs with bounded prefetch and manual acknowledgements
  -> atomically claim the attempt in Postgres
  -> broker-neutral inference processor
  -> ProviderRouter -> Ollama / configured OpenAI fallback
  -> result stored in Postgres -> acknowledge

Client -> GET /v1/jobs/:id -> JobQueue.getJob() -> status + result
```

`GET /health` is public and only reports that the gateway process is running. It does not check Postgres, Redis, RabbitMQ, or Ollama.

API keys are generated with 256 bits of randomness. Only their SHA-256 hashes and short display prefixes are stored in Postgres. The full key is printed once when it is created.

Rate-limit policies are stored per API key:

- `capacity` controls the maximum burst size.
- `refill-per-second` controls the sustained request rate.
- Redis evaluates each request atomically using its own server clock.

The gateway fails closed when the API-key store or rate limiter is unavailable. Authentication and per-key rate limiting apply to both chat routes, provider status, job submission, and job polling. Each HTTP request consumes one token; streamed chunks do not consume additional tokens.

Provider execution is protected separately by a process-local concurrency limiter:

- `PROVIDER_MAX_CONCURRENCY` controls active synchronous routing passes, shared by ordinary and streaming chat. One permit covers the primary call and any fallback call, including the full stream.
- `PROVIDER_MAX_QUEUE_SIZE` bounds the number of synchronous requests waiting for a slot.
- `PROVIDER_QUEUE_TIMEOUT_MS` bounds how long a request can wait.
- Queue overflow and wait timeout return `503` with `Retry-After`.

The synchronous wait queue is FIFO, bounded, and held only in gateway memory. It absorbs short bursts but is not a durable job queue. Each gateway replica has its own limit. Async jobs use RabbitMQ's single-active-consumer mode with prefetch set to `JOB_CONCURRENCY`. One worker consumes at a time; additional workers are standbys and may also dispatch pending publications. All workers should use the same concurrency setting. With one gateway and the defaults, normal capacity is four Ollama calls: two synchronous calls plus two async jobs. Size these limits together for your provider. Connection loss and lease recovery can temporarily overlap old and new executions; this is not an exactly-once execution guarantee.

Successful admissions emit structured fields for queue wait time, active executions, and queued requests. Metrics count admitted, rejected, timed-out, and cancelled acquisitions, observe wait times, and expose live occupancy. Disconnects remove queued waiters or abort active provider requests; logs distinguish cancellation during queueing from execution. Acquired permits are always released in `finally`.

### Provider routing and circuit breakers

Both HTTP chat and job workers use `ProviderRouter`, which implements `LLMProvider`. Each routing pass tries Ollama once. Connection failures, timeouts, HTTP `429`, and HTTP `5xx` permit one fallback call if the requested model has an explicit hosted mapping. An open Ollama circuit also permits fallback. Other errors, including upstream `400`/`401`/`404`, invalid responses, and unexpected application errors, surface immediately. If fallback fails, its error determines the HTTP response and whether an async attempt is retryable. There are no hidden adapter retries.

Streaming uses the same policy only **before the first nonempty text delta is handed to the client writer**. Once output starts, the router never retries or switches models. Client cancellation and downstream write failures do not count as provider health outcomes or trigger fallback. An abandoned half-open probe releases its probe slot so the next request can test recovery immediately; it does not reset the failure count.

Each provider has a separate process-local circuit:

- **Closed:** accept requests; open after five consecutive transient failures by default. A successful call or non-transient error resets the count. Here, closed means calls are allowed; it does not prove the provider is healthy.
- **Open:** skip calls for 30 seconds by default. Already running calls continue, but their late outcomes cannot change the newer circuit state.
- **Half-open:** after cooldown, the next request becomes the single recovery probe. Concurrent requests skip that provider. Success or a non-transient response closes the circuit; a transient failure starts another cooldown.

There are no background probes. An open circuit remains open in snapshots until a request starts its recovery probe. Circuit state resets on process restart and is independent in each gateway and worker. The breaker is shared across models within that provider, so a model-specific `429` can temporarily affect other models. It does not impose a shared cluster-wide quota.

Hosted fallback is disabled by default. Configure both variables in your local `.env` to enable it:

```dotenv
OPENAI_API_KEY=your-server-side-api-key
OPENAI_FALLBACK_MODEL_MAP='{"llama3.2":"your-openai-chat-model"}'
```

Replace `your-openai-chat-model` with a Chat Completions model available to your account. The map pairs the exact incoming Ollama model name with its hosted replacement; multiple entries are allowed. A key alone, a blank map, or `{}` keeps fallback disabled. A non-empty map without a key fails startup. Models absent from the map stay on Ollama, even during an outage. Gateway and worker should use the same mapping and timeout settings.

Enabling a mapping allows conversations for that model to be sent to OpenAI and incur its usage charges on fallback. Different models can produce different answers. The adapter uses the fixed HTTPS [Chat Completions endpoint](https://developers.openai.com/api/reference/resources/chat/subresources/completions/methods/create), sends `store: false`, and maps the first assistant message back to the existing response shape. Upstream refusal text is treated as a completed answer. A timed-out primary call may still be running remotely when fallback starts; this is not exactly-once inference.

Inspect the current gateway's circuits using an authenticated, rate-limited request:

```bash
curl http://localhost:3000/v1/providers \
  --header "authorization: Bearer ${API_KEY}"
```

```json
{
  "scope": "process",
  "providers": [
    { "name": "ollama", "state": "closed", "consecutiveFailures": 0, "retryAfterMs": 0 }
  ]
}
```

OpenAI appears only when enabled. This endpoint returns a snapshot with `Cache-Control: no-store`; it makes no provider calls, exposes no credentials, and does not report worker circuit state. Worker provider logs include the job's original request ID and circuit state. `/health` remains process liveness only. Open circuits without a usable fallback return `503 PROVIDER_CIRCUIT_OPEN` with `Retry-After`.

One execution can take up to the sum of both configured provider timeouts (120s + 30s by default when fallback is enabled), plus local overhead. Async leases add a 30s margin. Synchronous requests can also wait for a concurrency permit before execution. Async retries repeat the routing pass and remain bounded by `JOB_MAX_ATTEMPTS`. If a circuit is open, the durable retry delay is at least its remaining cooldown, even when that exceeds the normal backoff cap. A skipped routing pass still consumes a job attempt. Gateway shutdown cancels active and queued synchronous chat, including SSE; worker shutdown still drains accepted jobs. The default Compose shutdown grace is 180s; increase `PROVIDER_STOP_GRACE_PERIOD` when increasing provider timeouts to allow worker draining.

### Swappable queue adapter

`src/queue/job-queue.ts` defines `JobQueue<TData, TResult>` with `enqueue()` and `getJob()`, plus a `JobProcessor` callback and a worker lifecycle contract (`run()` / `close()`). Routes and `src/jobs/inference-processor.ts` have no broker dependency. `src/queue/rabbitmq.ts` implements the adapter and worker; `rabbitmq-broker.ts` contains AMQP operations, and `postgres-job-store.ts` manages durable job state. `server.ts` and `worker.ts` select these implementations at startup.

Another broker can replace this adapter while preserving ownership, idempotency, status/result storage, retries, and graceful draining. Redis serves rate limiting and optional exact-response caching, not jobs. Jobs from the earlier Redis queue implementation are not migrated automatically.

### Why the job table also acts as an outbox

Saving a job and independently publishing a message creates a gap: either operation can succeed while the other fails. Submission instead commits the job and `dispatch_pending = true` together. The worker dispatcher locks due rows using `FOR UPDATE SKIP LOCKED`, publishes persistent job-ID messages, waits for a broker confirm, and only then clears the flag and commits. A crash before commit leaves the job pending. Duplicate publication is safe because a conditional Postgres update allows only one worker to claim a particular attempt.

Retry delays are stored as `available_at` timestamps in Postgres. A transient failure schedules the next attempt durably before acknowledging the old delivery; the dispatcher publishes it when due. This avoids a separate broker delay plugin. Dispatch runs roughly once per second, so retry delays are minimum waits, not precise timers.

## Run with Docker Compose

This Compose setup contains gateway, worker, RabbitMQ, Redis, and Ollama. It uses your existing host Postgres; it does not manage a Postgres container. Create `.env` from `.env.example` if needed and set `DOCKER_DATABASE_URL` to your host database credentials using `host.docker.internal` as the hostname. Ensure that database is accessible from Docker.

Build and start the services:

```bash
docker compose up -d --build gateway rabbitmq
```

Pull the model into Ollama's persistent volume:

```bash
docker compose exec ollama ollama pull llama3.2
```

Run the idempotent database migration, then create an API key with a burst capacity of 10 requests and a refill rate of 1 request per second:

```bash
docker compose exec gateway node dist/scripts/migrate.js
docker compose exec gateway node dist/scripts/create-api-key.js local-dev 10 1
```

Save the `mm_...` key printed by the second command. It cannot be recovered from the database later.

The migration command applies all SQL migrations, including `002_create_inference_jobs.sql`, to the configured host database. Then start the worker:

```bash
docker compose up -d --build worker
```

The worker connects to Postgres, RabbitMQ, and Ollama. RabbitMQ's local management UI is at `http://localhost:15672`, with the Compose development credentials `modelmux` / `modelmux`. The main queue is `modelmux-inference`, and rejected deliveries go to `modelmux-inference.failed`.

## Call the API

Replace the value below with the key returned by the creation command:

```bash
API_KEY='mm_your_key_here'

curl --request POST http://localhost:3000/v1/chat \
  --header "authorization: Bearer ${API_KEY}" \
  --header 'content-type: application/json' \
  --data '{
    "model": "llama3.2",
    "messages": [
      {
        "role": "user",
        "content": "Explain TCP in one sentence."
      }
    ]
  }'
```

A successful response has a provider-neutral shape:

```json
{
  "message": {
    "role": "assistant",
    "content": "..."
  }
}
```

Rate-limited requests return `429` and include `Retry-After`, `X-RateLimit-Limit`, and `X-RateLimit-Remaining` headers.

### Opt into exact caching

Only ordinary `POST /v1/chat` supports caching. Add `X-Cache-Mode: use` to reuse a completed answer for the same API-key owner and exact validated request:

```bash
curl --include http://localhost:3000/v1/chat \
  --header "authorization: Bearer ${API_KEY}" \
  --header 'content-type: application/json' \
  --header 'x-cache-mode: use' \
  --data '{"model":"llama3.2","messages":[{"role":"user","content":"Explain TCP in one sentence."}]}'
```

`X-Cache` reports `HIT`, `MISS`, or `BYPASS`. A miss does not guarantee a write: oversized answers and cache-write failures are skipped. Missing `X-Cache-Mode`, explicit `bypass`, or request `Cache-Control: no-store`/`no-cache` bypass both reads and writes. Bypass does not delete an existing entry. Invalid cache modes return `400`. Responses use `Cache-Control: no-store` to prevent intermediary HTTP caching.

Authentication, rate limiting, and validation always precede lookup. Hits skip provider capacity and routing, preserve fresh HTTP request IDs/rate-limit headers, and do not change circuit health. Keys hash the API-key owner, complete ordered conversation, model, cache version, and routing configuration. Object property order is ignored; message order, roles, whitespace, and content are not. Shared API keys share a cache scope—this is not separate end-user isolation.

Successful routed answers, including fallback answers, can be reused until expiration even if the primary recovers. No errors or partial streams are cached. Concurrent misses can still run multiple inferences; atomic Redis `SET EX NX` keeps the first stored answer without renewing its lifetime. Cache reads have bounded byte counts and validate the stored response. Cache commands default to a 100ms budget; read errors bypass caching and write errors preserve the generated answer. Cancellation interrupts waiting, but cannot retract a Redis write already sent. Provider capacity is released before cache writes.

Default retention is five minutes and the serialized answer limit is 65,536 UTF-8 bytes. Set `CHAT_CACHE_TTL_SECONDS=0` to disable globally. Bump `CHAT_CACHE_VERSION` after in-place model/template changes; old entries become unreachable and expire normally. TTL changes affect new entries, not old ones. Streaming and async jobs are unchanged; this is exact matching, not semantic caching or in-flight deduplication.

Redis now stores answer text, which can contain private information. Hashed keys are not encryption; TTL is not secure erasure from AOF files/backups. The cache uses a separate connection but shares Redis capacity with rate limiting. Per-entry bounds and TTL do not impose a global memory cap. Preserve the existing `noeviction` policy for limiter correctness; a full Redis instance can still make rate limiting fail closed even though cache errors are ignored. Separate capacity planning is needed before production use.

### Stream an answer

Use the same request body and bearer key with `POST /v1/chat/stream`. `curl -N` disables curl's output buffering:

```bash
curl -N --request POST http://localhost:3000/v1/chat/stream \
  --header "authorization: Bearer ${API_KEY}" \
  --header 'content-type: application/json' \
  --data '{"model":"llama3.2","messages":[{"role":"user","content":"Explain TCP in one sentence."}]}'
```

The response is `text/event-stream`, with JSON data in named SSE events:

```text
event: delta
data: {"content":"TCP ","requestId":"example-request-id"}

event: delta
data: {"content":"delivers bytes reliably.","requestId":"example-request-id"}

event: done
data: {"requestId":"example-request-id"}

```

Append each `delta.content` to the displayed answer. A delta is a text fragment, not necessarily one token. `done` means the provider stream ended normally; an empty answer still receives `done`. Token usage and provider finish reasons are not exposed yet, so `done` does not distinguish a natural stop from a provider output-length limit.

Before any text is emitted, failures use the normal JSON error response and HTTP status. After output starts, the status is already `200`; failures produce a sanitized `error` event when the socket is still writable, then close without `done`:

```text
event: error
data: {"code":"PROVIDER_TIMEOUT","message":"The response could not be completed","requestId":"example-request-id"}

```

Clients must treat `error` **or EOF without `done`** as an incomplete answer. A disconnected or stalled socket may close without an error event. There is no replay, resume, or automatic retry of a partial answer. In a browser, use `fetch` and read its response stream; native `EventSource` cannot send this POST body and bearer header.

Streaming goes directly through the gateway, not RabbitMQ: no worker-to-gateway return channel is needed, and accepted async jobs retain their independent lifecycle. `LLMProvider.stream()` awaits each text consumer callback. The adapters decode [Ollama NDJSON](https://docs.ollama.com/api/streaming) and [OpenAI Chat Completions SSE](https://developers.openai.com/api/docs/guides/streaming-responses) incrementally, including UTF-8 split across network chunks, and require provider terminal markers. Incomplete or malformed streams are not silently treated as successful.

When the downstream socket buffer fills, the gateway waits for `drain` before consuming more provider text. A 15-second stalled-write limit cancels the request. Provider timeouts cover the entire call, including streamed body reads and downstream waits; whichever deadline expires first wins. An overall provider timeout still counts toward the circuit breaker, even if downstream waiting contributed to that duration. Parsers bound each line/SSE data event to 65,536 decoded characters, retain at most the unfinished frame plus the current network chunk, and never accumulate the full answer. Transport buffers still exist on both sides.

Closing the client connection, cancelling a browser fetch, or shutting down the gateway aborts upstream HTTP and releases gateway capacity. Remote inference may not stop immediately; cancelling transport is not a guarantee that the provider stops computing. SSE sends `Cache-Control: no-cache, no-transform` and `X-Accel-Buffering: no`; configure any reverse proxy to honor streaming and allow the intended timeout. No heartbeat is sent while waiting for capacity or the first text, so proxy idle timeouts must account for that delay.

### Submit and poll an async job

Use the same chat body and include a client-generated idempotency key:

```bash
curl --include --request POST http://localhost:3000/v1/jobs \
  --header "authorization: Bearer ${API_KEY}" \
  --header 'idempotency-key: tcp-explanation-1' \
  --header 'content-type: application/json' \
  --data '{"model":"llama3.2","messages":[{"role":"user","content":"Explain TCP in one sentence."}]}'
```

The gateway returns `202`, a `Location: /v1/jobs/job-...` header, and `{"id":"job-...","requestId":"..."}`. Copy the returned ID to poll using the same API key:

```bash
JOB_ID='job_REPLACE_WITH_RETURNED_ID'
curl "http://localhost:3000/v1/jobs/${JOB_ID}" \
  --header "authorization: Bearer ${API_KEY}"
```

Statuses are `queued`, `active`, `retrying`, `completed`, and `failed`. A completed job returns:

```json
{
  "id": "job-...",
  "requestId": "original-request-id",
  "status": "completed",
  "attemptsMade": 1,
  "response": { "message": { "role": "assistant", "content": "..." } }
}
```

Failed jobs return HTTP `200` with `status: "failed"` and a safe `error: {code, message}` instead of `response`. Looking up another API key's job returns the same `404` as a missing job. Poll about once per second with the example rate policy, and stop at a terminal status.

### Async behavior and tradeoffs

- **Idempotency:** a key is required (1–128 letters, digits, `.`, `_`, or `-`). Matching requests with the same key and API-key owner reuse one job, including while it is running. Different input with that key returns `409`. The complete model and ordered messages are compared; object property order does not matter. Different keys intentionally create different jobs.
- **Retention:** workers delete completed/failed Postgres records older than 24 hours during maintenance. Records remain longer if workers are stopped. Idempotency lasts while the row is retained; after cleanup the same key creates a fresh job ID. Pending jobs have no retention deadline. Dead-letter messages have a 24-hour RabbitMQ TTL.
- **Retries:** three total attempts by default, with exponential delays starting at one second (one second, then two seconds). Connection failures, provider timeouts, HTTP `429`, and upstream `5xx` retry. Other upstream `4xx`, invalid responses, and unexpected application errors fail immediately. Reusing a failed job's key returns that failed job; use a new key to deliberately submit new work.
- **Delivery:** publisher confirms protect publication; manual acknowledgements happen after saving the result or retry decision. A crash after inference but before saving can repeat inference. Attempts have a lease covering the sum of enabled provider timeouts plus 30000ms; expired attempts are rescheduled until the attempt budget is exhausted. Conditional updates prevent an old attempt from overwriting a newer result. See [RabbitMQ acknowledgement guarantees](https://www.rabbitmq.com/docs/confirms).
- **Durability:** Postgres holds prompts, results, idempotency, and pending publications. RabbitMQ holds persistent job-ID messages in durable quorum queues. The local single-node broker has no replica redundancy; quorum queues need a multi-node deployment to tolerate node loss. See [RabbitMQ quorum queues](https://www.rabbitmq.com/docs/quorum-queues).
- **Failures and dead letters:** malformed deliveries and permanent/exhausted inference failures are rejected to the failed queue. Polling Postgres remains authoritative; a crash between recording a terminal failure and rejecting the delivery can omit its dead-letter copy. Infrastructure failures stop the worker with deliveries unacknowledged. Broker redelivery counts are disabled (`x-delivery-limit=-1`); Postgres bounds inference attempts, while worker shutdown avoids a tight requeue loop during infrastructure outages.
- **Backlog:** accepted jobs survive worker/broker outages in Postgres. The gateway can return `202` while RabbitMQ is down; publication resumes when a worker reconnects. Database failures return `503`. This milestone does not cap the async backlog, so prolonged downtime or sustained overload can still consume database/broker storage.
- **Lifecycle:** client disconnects do not cancel accepted jobs. On shutdown, the worker drains active calls before cancelling its consumer, preventing a standby from starting extra calls during normal graceful handover. Any newly delivered messages are left unacknowledged for redelivery. Compose allows 180 seconds by default to drain primary plus fallback calls and restarts failed workers. Host processes must be restarted after broker/database connection failures; an in-process reconnect supervisor is deferred.
- **Logging:** the original request ID follows gateway → job → worker, alongside job ID, owner ID, attempt number, and processing time. Duplicate submissions keep the original job request ID; each HTTP call still has its own `X-Request-ID`.

## Run the gateway on the host

Node.js 20 or newer is required. Start Redis, RabbitMQ, and Ollama with Docker, then point `DATABASE_URL` in `.env` at the existing local Postgres instance:

```bash
docker compose up -d redis rabbitmq ollama
docker compose exec ollama ollama pull llama3.2
npm install
cp .env.example .env
# Edit DATABASE_URL in .env if your local credentials differ from the default.
npm run db:migrate
npm run api-key:create -- local-dev 10 1
npm run dev
```

In another terminal, start the worker:

```bash
npm run worker:dev
```

For compiled execution, run `npm run build`, then `npm start` and `npm run worker` in separate terminals.

The final three arguments to `api-key:create` are the key name, burst capacity, and tokens refilled per second.

## Observability

Gateway and worker each expose a separate internal `/metrics` listener. Host defaults are `127.0.0.1:9464` and `127.0.0.1:9465` respectively:

```bash
curl http://127.0.0.1:9464/metrics
curl http://127.0.0.1:9465/metrics
```

These listeners do not require an API key or consume rate-limit tokens. The public API port does **not** serve `/metrics`. Scraping does not invoke providers. Listener failures log a warning without stopping inference, and metric recording is best-effort: telemetry errors must not change admission, routing, cancellation, persistence, or acknowledgement decisions. Metrics are not an audit ledger.

### Prometheus and Grafana

The supplied scrape configuration targets the **Compose** gateway and worker, not host-running Node processes. After completing the Compose setup and database migration above, rebuild those services and start the optional monitoring profile:

```bash
docker compose up -d --build gateway worker
docker compose --profile observability up -d prometheus grafana
```

- Prometheus: `http://localhost:9090`; check that both `modelmux` targets are up.
- Grafana: `http://localhost:3001`; open **ModelMux → ModelMux Overview**.
- Local Grafana login: `admin` / `modelmux-local-only`, unless `GRAFANA_ADMIN_PASSWORD` was set before the Grafana database was first initialized. Changing that variable does not reset an existing account's password.

Datasource and dashboard provisioning live under `observability/`; edit those versioned files rather than the Grafana UI. Prometheus scrapes every 15 seconds and retains samples for seven days in a named volume. Grafana configuration also persists in a named volume. See [Grafana provisioning](https://grafana.com/docs/grafana/latest/administration/provisioning/) for the file-based setup.

Compose binds metrics on its container network without publishing their ports; only the monitoring UIs are published, on host loopback. Host-running Node listeners also default to loopback. Keep these unauthenticated endpoints private: non-local deployments need deliberate network restrictions and authenticated/TLS access. The development Grafana password is not suitable for shared use. To monitor host-running Node from a container, configure networking and scrape targets explicitly; container `localhost` is not the host.

### Reading the metrics

Every application metric starts with `modelmux_` and has `service="gateway"` or `service="worker"`. Prometheus adds target labels such as `instance`. Labels are bounded: route templates, fixed methods/outcomes, and provider names. No prompts, answers, keys, owner IDs, job IDs, request IDs, arbitrary model names, or raw URL paths become metric labels. Use the existing correlated structured logs to investigate an individual request.

| Metric family (omit `modelmux_` below) | Meaning |
| --- | --- |
| `http_requests_total`, `http_duration_seconds`, `http_active` | HTTP transport outcomes, lifetime, and in-flight requests, including authentication/validation failures |
| `rate_limit_rejections_total` | Per-key token bucket rejections; distinct from concurrency rejection |
| `admissions_total`, `queue_wait_seconds`, `concurrency_active`, `concurrency_queued` | Gateway FIFO outcomes, wait, occupied permits, and waiting requests |
| `provider_calls_total`, `provider_duration_seconds`, `provider_active` | Actual provider calls by provider, `chat`/`stream`, and outcome |
| `circuit_rejections_total`, `fallbacks_total`, `circuit_state` | Skipped calls, fallback routing decisions, and one-hot process-local circuit snapshots |
| `cache_lookups_total` | Ordinary chat handler decisions: `HIT`, `MISS`, or `BYPASS` |
| `streams_total` | SSE handler outcomes: `completed`, `failed`, or `cancelled` |
| `jobs_depth` | Shared Postgres jobs by nonterminal status: `queued`, `retrying`, `active` |
| `job_attempts_total`, `job_attempt_duration_seconds`, `job_attempts_active` | Worker-local claimed attempt outcomes, execution/persistence duration, and active attempts |
| `job_queue_wait_seconds`, `job_retries_started_total` | Availability-to-claim wait and claimed attempts numbered greater than one |
| `collector_up` | Last snapshot collection succeeded (`1`) or failed (`0`), by collector |

Important interpretation details:

- Durations are **seconds**. HTTP latency includes auth, queueing, and the entire stream; provider streaming latency includes awaited client writes. Neither is time to first token. The dashboard derives p50/p95 from histogram buckets, not exact request samples. Low/no traffic can yield sparse or missing series; labelled counters appear only after their first observation. See [Prometheus histograms](https://prometheus.io/docs/practices/histograms/).
- An HTTP `completed` transport with status `200` can still contain an SSE error. Inspect `streams_total` as well: its `completed` means the server successfully wrote `done`, not that the client acknowledged receiving it. Auth/validation failures do not enter the streaming handler. Disconnected transports use outcome `aborted`; status is `none` if headers were never sent, rather than a fabricated HTTP error code.
- Actual provider outcomes distinguish `success`, `transient_error`, `permanent_error`, `cancelled`, and `consumer_error`. Circuit skips have a separate counter and do not inflate provider latency. A fallback decision can itself meet an open fallback circuit. Circuit snapshots never trigger probes and do not prove dependency readiness.
- Gateway FIFO depth is process-local. `jobs_depth` instead counts durable Postgres rows, including scheduled retries and unpublished jobs; it is **not RabbitMQ delivery depth**. Only gateways collect it, once per scrape. Multiple gateways see the same backlog: use `max by (status)`, not `sum`. The query adds database work proportional to nonterminal backlog, so revisit it under measured load.
- Queue-depth collection has a one-second wait budget. Failure emits unavailable (`NaN`) depths and `collector_up{collector="jobs"}=0`, not a false zero. It does not make `/metrics` require healthy auth/Redis. The driver timeout does not cancel database execution; the existing pool's ten-second statement timeout remains the server-side bound. Prometheus `up=1` only proves scraping worked, not that every collector or dependency is healthy.
- Worker attempt timing includes result/retry persistence but excludes claiming and broker acknowledgement. Retry counts measure attempts actually started, not retries merely scheduled. `success` means the completion write returned without error; a fenced stale write may be a no-op. Counts are not distinct completed jobs or exactly-once execution. Queue wait starts at `available_at`, excluding intentional retry backoff.
- Token usage and estimated cost are deliberately **not reported**: the provider-neutral contract does not yet carry trustworthy usage metadata. Text fragments are not tokens. No performance or cost numbers are fabricated.

The metrics client is `prom-client` 15, compatible with this repository's current Node 20 runtime. npm marks it deprecated in favor of `@prometheus-io/client`; the [replacement's manifest](https://github.com/prometheus/client_js/blob/main/package.json) currently requires Node 22/24/26+. A runtime/client migration is deferred rather than silently changing the project's runtime in this milestone.

Verification covers metric values, label privacy, cancellation, failure isolation, real localhost scraping, and static dashboard/metric references. Compose configuration validates, but a live Prometheus scrape and Grafana render are still unverified because the local Docker daemon was unavailable. Postgres backlog SQL also awaits the existing database credential fix; see `HANDOFF.md`.

## Configuration

| Variable | Local default | Purpose |
| --- | --- | --- |
| `PORT` | `3000` | Gateway listen port |
| `METRICS_HOST` | `127.0.0.1` | Internal metrics bind address; Compose overrides to its container network |
| `GATEWAY_METRICS_PORT` | `9464` | Gateway-only metrics listener; Compose fixes this port for its scrape target |
| `WORKER_METRICS_PORT` | `9465` | Worker-only metrics listener; Compose fixes this port for its scrape target |
| `GRAFANA_ADMIN_PASSWORD` | `modelmux-local-only` | Initial Compose Grafana admin password; local development only |
| `OLLAMA_BASE_URL` | `http://localhost:11434` | Ollama server URL |
| `OLLAMA_REQUEST_TIMEOUT_MS` | `120000` | Entire Ollama call deadline, including streaming and downstream waits |
| `OPENAI_API_KEY` | unset | Server-side hosted credential; a key alone does not enable fallback |
| `OPENAI_FALLBACK_MODEL_MAP` | unset | JSON object mapping incoming local model names to hosted replacements; explicit opt-in |
| `OPENAI_REQUEST_TIMEOUT_MS` | `30000` | Entire hosted call deadline, including streaming, when fallback is enabled |
| `CIRCUIT_BREAKER_FAILURE_THRESHOLD` | `5` | Consecutive transient failures before opening each circuit |
| `CIRCUIT_BREAKER_RESET_TIMEOUT_MS` | `30000` | Cooldown before one recovery request is admitted |
| `PROVIDER_STOP_GRACE_PERIOD` | `180s` | Compose gateway/worker shutdown grace; size for configured timeouts |
| `DATABASE_URL` | `postgresql://modelmux:modelmux@localhost:5432/modelmux` | API keys, job state, results, and pending publications |
| `REDIS_URL` | `redis://localhost:6379` | Token-bucket state and optional exact cache |
| `CHAT_CACHE_TTL_SECONDS` | `300` | New entry lifetime; `0` disables caching globally |
| `CHAT_CACHE_MAX_ENTRY_BYTES` | `65536` | Maximum serialized cached answer size in UTF-8 bytes |
| `CHAT_CACHE_TIMEOUT_MS` | `100` | Per-cache-command wait budget |
| `CHAT_CACHE_VERSION` | `1` | Bump to invalidate lookup after in-place model/template changes |
| `RABBITMQ_URL` | `amqp://modelmux:modelmux@localhost:5672` | Worker AMQP connection (`amqp` or `amqps`) |
| `PROVIDER_MAX_CONCURRENCY` | `2` | Maximum active provider calls in this gateway process |
| `PROVIDER_MAX_QUEUE_SIZE` | `20` | Maximum synchronous requests waiting for provider capacity |
| `PROVIDER_QUEUE_TIMEOUT_MS` | `30000` | Maximum time a request may wait for provider capacity |
| `JOB_CONCURRENCY` | `2` | Prefetch for the single active consumer |
| `JOB_MAX_ATTEMPTS` | `3` | Total attempts for newly submitted jobs |
| `JOB_BACKOFF_MS` | `1000` | Initial exponential retry delay |
| `DOCKER_DATABASE_URL` | `postgresql://modelmux:modelmux@host.docker.internal:5432/modelmux` | Compose gateway/worker connection to host Postgres |

Values from a local `.env` file are loaded when present.

## HTTP errors

| Status | Code | Meaning |
| --- | --- | --- |
| `400` | `VALIDATION_ERROR` | Request body, headers, or parameters failed schema validation |
| `401` | `INVALID_API_KEY` | The bearer API key is missing, malformed, disabled, or unknown |
| `404` | `JOB_NOT_FOUND` | Job is missing, removed, or belongs to another API key |
| `409` | `IDEMPOTENCY_CONFLICT` | The same key was used with different input |
| `429` | `RATE_LIMIT_EXCEEDED` | The API key has exhausted its token bucket |
| `501` | `STREAMING_UNSUPPORTED` | A selected custom provider does not implement streaming |
| `502` | `PROVIDER_ERROR` | The last attempted provider returned an unsuccessful response |
| `502` | `INVALID_PROVIDER_RESPONSE` | A provider returned a malformed successful response |
| `503` | `API_KEY_STORE_UNAVAILABLE` | Postgres could not authenticate the request |
| `503` | `RATE_LIMITER_UNAVAILABLE` | Redis could not evaluate the request |
| `503` | `JOB_QUEUE_UNAVAILABLE` | The queue could not accept or retrieve a job |
| `503` | `PROVIDER_UNAVAILABLE` | The gateway could not connect to the last attempted provider |
| `503` | `PROVIDER_CIRCUIT_OPEN` | A provider was skipped during cooldown or an active recovery probe; includes `Retry-After` |
| `503` | `CONCURRENCY_QUEUE_FULL` | The bounded provider wait queue is full |
| `503` | `CONCURRENCY_WAIT_TIMEOUT` | A request waited too long for provider capacity |
| `504` | `PROVIDER_TIMEOUT` | The last attempted provider exceeded its configured timeout |

Error responses include the request ID. For streaming, these HTTP statuses only apply before SSE output begins; later failures use the `error` event described above. Ollama upstream error details may appear in structured logs; OpenAI response bodies and low-level error causes are discarded to avoid echoing hosted credentials or prompts. Neither is returned in public error details.

## Development commands

```bash
npm test
npm run typecheck
npm run build
npm run check
npm run test:streaming
npm run test:metrics
npm run test:cache
```

Unit and injection tests mock queue/provider boundaries and need no running services. `test:streaming` separately starts ephemeral localhost HTTP servers with stub upstream responses to verify incremental delivery, disconnect cancellation, capacity recovery, and shutdown; it needs socket permissions but no database, broker, Ollama, or hosted key. Unit tests also exercise backpressure, parsing, fallback boundaries, and cancellation-safe circuit state.

`test:metrics` opens ephemeral API and metrics listeners to verify unauthenticated scraping remains separate from API authentication/rate limiting. It also needs socket permissions but no external services. Static dashboard tests verify known metric names, datasource/target wiring, histogram aggregation, and shared-backlog aggregation; they are not a PromQL parser or a live Grafana test.

`test:cache` uses configured Redis, creates one random-namespaced test key, verifies atomic expiry/isolation/size limits, and deletes only that key. It never flushes Redis or calls a model. Its last run could not connect to Redis; unit tests alone do not verify live Redis behavior.

Integration tests use the configured local `DATABASE_URL` and `RABBITMQ_URL` to exercise real SQL concurrency, confirmed publication, retries, dead letters, results, and bounded consumption across two workers, using a stub provider:

```bash
docker compose up -d rabbitmq
npm run test:integration
# To verify only the Postgres job store:
npm run test:store
```

Tests create and remove a random Postgres schema and random RabbitMQ queues. Your configured database user needs permission to create a schema. Tests do not alter application tables, manage Postgres through Docker, or call Ollama. Run `npm run db:migrate` separately to install the application schema before starting the gateway/worker.

## Project structure

```text
.
├── db/migrations              # Idempotent Postgres schema
├── observability              # Prometheus config and provisioned Grafana dashboard
├── src
│   ├── app.ts                 # Fastify app factory and error handling
│   ├── cancellation.ts        # Shared cancellation error and abortable waits
│   ├── config.ts              # Environment configuration
│   ├── errors.ts              # Stable application errors
│   ├── server.ts              # Runtime composition and listener
│   ├── worker.ts              # Separate async worker entry point
│   ├── auth
│   │   ├── api-keys.ts        # Key hashing and authentication contract
│   │   ├── guard.ts           # Protected-route authentication and limiting
│   │   └── postgres-api-key-store.ts
│   ├── concurrency
│   │   ├── concurrency-limiter.ts
│   │   └── in-memory-concurrency-limiter.ts
│   ├── cache                 # Owner-scoped exact keys and bounded Redis cache
│   ├── observability
│   │   ├── telemetry.ts      # Best-effort metric events; no business-policy decisions
│   │   ├── metrics.ts        # Per-process registry and bounded snapshot collectors
│   │   ├── http.ts           # HTTP completion/disconnect instrumentation
│   │   └── server.ts         # Separate internal /metrics listener
│   ├── providers
│   │   ├── create-provider.ts # Shared gateway/worker composition
│   │   ├── router.ts          # Primary-first routing and explicit fallback maps
│   │   ├── circuit-breaker.ts # Per-process closed/open/half-open state
│   │   ├── failure-policy.ts  # Shared fallback and job retry classification
│   │   ├── http-request.ts    # Whole-call deadlines and upstream cancellation
│   │   ├── stream-parser.ts   # Bounded incremental NDJSON/SSE decoding
│   │   ├── ollama.ts          # Ollama HTTP adapter
│   │   ├── openai.ts          # Optional hosted Chat Completions adapter
│   │   └── provider.ts        # Provider interface
│   ├── jobs
│   │   ├── inference-jobs.ts  # Chat-specific queue type
│   │   └── inference-processor.ts # Broker-neutral provider execution
│   ├── queue
│   │   ├── job-queue.ts       # Generic queue and worker contracts
│   │   ├── job-error.ts       # Safe job errors and retry decisions
│   │   ├── postgres-job-store.ts # Idempotency, outbox, leases, and results
│   │   ├── rabbitmq-broker.ts # AMQP connection, confirms, acknowledgements
│   │   └── rabbitmq.ts        # RabbitMQ queue/worker adapter
│   ├── rate-limit
│   │   ├── rate-limiter.ts    # Provider-neutral limiter contract
│   │   └── redis-token-bucket.ts
│   ├── routes
│   │   ├── chat.ts           # Synchronous chat and direct SSE streaming
│   │   ├── chat-schema.ts    # Shared chat schemas
│   │   ├── request-cancellation.ts # Disconnect and shutdown signals
│   │   ├── sse.ts            # SSE framing and bounded drain waits
│   │   ├── providers.ts      # Protected process-local circuit snapshots
│   │   └── jobs.ts           # Async submission and polling
│   ├── scripts               # Migration and API-key commands
│   └── types/chat.ts          # Chat request/response types
├── test                       # Injection and unit tests
├── Dockerfile
├── docker-compose.yml
├── package.json
└── .env.example
```

Exact caching and observability are implemented; live infrastructure checks remain outstanding. Load testing (Milestone 9), final polish (Milestone 10), and optional pgvector experiments require follow-up work. Cancellation and remaining verification items are tracked in `TODO.md`.
