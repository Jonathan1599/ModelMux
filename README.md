# ModelMux

ModelMux is a provider-agnostic LLM inference gateway built with Node.js, TypeScript, and Fastify. It authenticates clients with API keys, applies a distributed per-key token bucket, and supports synchronous chat and asynchronous inference jobs backed by RabbitMQ and Postgres.

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
       -> bounded process-local concurrency limiter
       -> ProviderRouter -> Ollama (primary) -> ChatResponse
                         -> OpenAI (configured fallback on transient failures)
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

The gateway fails closed when the API-key store or rate limiter is unavailable. Authentication and per-key rate limiting apply to `/v1/chat`, job submission, and job polling. Polling and repeated submissions each consume a token.

Provider execution is protected separately by a process-local concurrency limiter:

- `PROVIDER_MAX_CONCURRENCY` controls active synchronous routing passes. One permit covers the primary call and any fallback call.
- `PROVIDER_MAX_QUEUE_SIZE` bounds the number of synchronous requests waiting for a slot.
- `PROVIDER_QUEUE_TIMEOUT_MS` bounds how long a request can wait.
- Queue overflow and wait timeout return `503` with `Retry-After`.

The synchronous wait queue is FIFO, bounded, and held only in gateway memory. It absorbs short bursts but is not a durable job queue. Each gateway replica has its own limit. Async jobs use RabbitMQ's single-active-consumer mode with prefetch set to `JOB_CONCURRENCY`. One worker consumes at a time; additional workers are standbys and may also dispatch pending publications. All workers should use the same concurrency setting. With one gateway and the defaults, normal capacity is four Ollama calls: two synchronous calls plus two async jobs. Size these limits together for your provider. Connection loss and lease recovery can temporarily overlap old and new executions; this is not an exactly-once execution guarantee.

Successful admissions emit structured fields for queue wait time, active executions, and queued requests. The limiter also tracks admitted, rejected, timed-out, total-wait, and maximum-wait values for the later metrics milestone.

### Provider routing and circuit breakers

Both HTTP chat and job workers use `ProviderRouter`, which implements `LLMProvider`. Each routing pass tries Ollama once. Connection failures, timeouts, HTTP `429`, and HTTP `5xx` permit one fallback call if the requested model has an explicit hosted mapping. An open Ollama circuit also permits fallback. Other errors, including upstream `400`/`401`/`404`, invalid responses, and unexpected application errors, surface immediately. If fallback fails, its error determines the HTTP response and whether an async attempt is retryable. There are no hidden adapter retries.

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

One execution can take up to the sum of both configured provider timeouts (120s + 30s by default when fallback is enabled), plus local overhead. Async leases add a 30s margin. Synchronous requests can also wait for a concurrency permit before execution. Async retries repeat the routing pass and remain bounded by `JOB_MAX_ATTEMPTS`. If a circuit is open, the durable retry delay is at least its remaining cooldown, even when that exceeds the normal backoff cap. A skipped routing pass still consumes a job attempt. The default Compose shutdown grace is 180s; increase `PROVIDER_STOP_GRACE_PERIOD` when increasing provider timeouts, and account for queued synchronous work during draining.

### Swappable queue adapter

`src/queue/job-queue.ts` defines `JobQueue<TData, TResult>` with `enqueue()` and `getJob()`, plus a `JobProcessor` callback and a worker lifecycle contract (`run()` / `close()`). Routes and `src/jobs/inference-processor.ts` have no broker dependency. `src/queue/rabbitmq.ts` implements the adapter and worker; `rabbitmq-broker.ts` contains AMQP operations, and `postgres-job-store.ts` manages durable job state. `server.ts` and `worker.ts` select these implementations at startup.

Another broker can replace this adapter while preserving ownership, idempotency, status/result storage, retries, and graceful draining. Redis remains dedicated to rate limiting. Jobs from the earlier Redis queue implementation are not migrated automatically.

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

## Configuration

| Variable | Local default | Purpose |
| --- | --- | --- |
| `PORT` | `3000` | Gateway listen port |
| `OLLAMA_BASE_URL` | `http://localhost:11434` | Ollama server URL |
| `OLLAMA_REQUEST_TIMEOUT_MS` | `120000` | Maximum non-streaming Ollama request duration |
| `OPENAI_API_KEY` | unset | Server-side hosted credential; a key alone does not enable fallback |
| `OPENAI_FALLBACK_MODEL_MAP` | unset | JSON object mapping incoming local model names to hosted replacements; explicit opt-in |
| `OPENAI_REQUEST_TIMEOUT_MS` | `30000` | Hosted call timeout when fallback is enabled |
| `CIRCUIT_BREAKER_FAILURE_THRESHOLD` | `5` | Consecutive transient failures before opening each circuit |
| `CIRCUIT_BREAKER_RESET_TIMEOUT_MS` | `30000` | Cooldown before one recovery request is admitted |
| `PROVIDER_STOP_GRACE_PERIOD` | `180s` | Compose gateway/worker shutdown grace; size for configured timeouts |
| `DATABASE_URL` | `postgresql://modelmux:modelmux@localhost:5432/modelmux` | API keys, job state, results, and pending publications |
| `REDIS_URL` | `redis://localhost:6379` | Token-bucket state |
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

Error responses include the request ID. Ollama upstream error details may appear in structured logs; OpenAI response bodies and low-level error causes are discarded to avoid echoing hosted credentials or prompts. Neither is returned in public error details.

## Development commands

```bash
npm test
npm run typecheck
npm run build
npm run check
```

Unit and injection tests mock queue/provider boundaries and need no running services. Integration tests use the configured local `DATABASE_URL` and `RABBITMQ_URL` to exercise real SQL concurrency, confirmed publication, retries, dead letters, results, and bounded consumption across two workers, using a stub provider:

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
├── src
│   ├── app.ts                 # Fastify app factory and error handling
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
│   ├── providers
│   │   ├── create-provider.ts # Shared gateway/worker composition
│   │   ├── router.ts          # Primary-first routing and explicit fallback maps
│   │   ├── circuit-breaker.ts # Per-process closed/open/half-open state
│   │   ├── failure-policy.ts  # Shared fallback and job retry classification
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
│   │   ├── chat.ts           # Synchronous chat
│   │   ├── chat-schema.ts    # Shared chat schemas
│   │   └── jobs.ts           # Async submission and polling
│   ├── scripts               # Migration and API-key commands
│   └── types/chat.ts          # Chat request/response types
├── test                       # Injection and unit tests
├── Dockerfile
├── docker-compose.yml
├── package.json
└── .env.example
```

Caching, provider failover, circuit breakers, streaming, Prometheus, Grafana, pgvector, and load testing remain intentionally deferred to later milestones. Client-abort propagation for synchronous chat is tracked in `TODO.md`.
