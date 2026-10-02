# ModelMux agent handoff

Last updated: 2026-09-29. This is a snapshot; inspect `git status` and the current files before continuing.

## Where we stopped

Milestones 1–4 are committed. HEAD at the start of this session was `4a3918e` (`feat: add durable async inference jobs with RabbitMQ`), and the worktree was clean. Milestone 4 live verification remains outstanding: the configured local Postgres credentials are rejected.

The user requested the next milestone and said “Go.” Milestone 5 is implemented in the working tree: Ollama-first routing, optional OpenAI fallback, circuit breakers, and provider state. No commit or Milestone 6 work was requested. Hosted fallback remains opt-in; `.env` was not edited and no hosted inference requests were made.

The module configuration remains **CommonJS**, with extensionless TypeScript imports (`f1d85fa`). Preserve it unless asked to change it.

## User constraints and preferences

- Work in this repository root. The project name is **ModelMux**; do not create a nested `llm-gateway` directory.
- Use the user's existing local Postgres. **Do not start, stop, replace, or manage Postgres through Docker.** Host processes use `DATABASE_URL`; Compose gateway/worker use `DOCKER_DATABASE_URL` with `host.docker.internal`.
- Keep implementations readable and suitable for explaining in interviews. Avoid frameworks or abstractions without a concrete need.
- Preserve the generic queue contract so another broker can replace RabbitMQ without rewriting routes or inference processing.
- Use `response` for Fastify handler variables, following the user's naming preference.
- Preserve all existing modified and untracked work. The current diff contains Milestone 5 on top of the committed async milestone.
- Do not implement later milestones or commit changes automatically. See `AGENTS.md` for the roadmap and working preferences.

## Implemented behavior

Synchronous chat remains `POST /v1/chat`: API-key authentication backed by Postgres, Redis token-bucket rate limiting, process-local FIFO concurrency control, and the `LLMProvider` abstraction. A provider router now calls Ollama and optionally OpenAI. `/health` only reports that the gateway process is up.

### Milestone 5 routing behavior

- `src/providers/create-provider.ts` composes the same router in gateway and worker; every process has independent provider circuit state. The HTTP permit covers the full routing pass, while worker prefetch bounds async passes.
- Primary Ollama is called once per pass. Connection failures, timeouts, upstream `429`/`5xx`, or an open circuit permit one fallback call. Other upstream `4xx`, malformed responses, and unexpected application errors do not trigger fallback. The final target's error determines HTTP/job failure classification; a permanent hosted error ends an async attempt without retry even if the primary error was transient.
- `OPENAI_FALLBACK_MODEL_MAP` is an explicit JSON mapping of incoming local model names to hosted replacements. Missing/blank/empty mappings disable hosted fallback, even if an API key exists. A non-empty map requires `OPENAI_API_KEY`. Only own, exact matching map entries permit forwarding. Credentials and the map are not exposed by provider status.
- `OpenAIProvider` uses native fetch against the fixed HTTPS Chat Completions endpoint, blocks redirects, sends `stream:false` and `store:false`, and performs no internal retries. It validates the response at runtime; a textual refusal is a completed response. Hosted HTTP error bodies and raw causes are discarded. Official request/response documentation: https://developers.openai.com/api/reference/resources/chat/subresources/completions/methods/create . No SDK dependency was added.
- Circuits default to five consecutive transient failures and a 30s cooldown. A single real request probes after cooldown; concurrent callers skip that provider. Success or a non-transient error resets/closes the circuit. Generations prevent late outcomes from old calls from changing newer state. Snapshots do not advance the circuit or probe providers. Circuits are shared across that provider's models and reset on restart.
- `GET /v1/providers` is authenticated and rate-limited, returns `Cache-Control:no-store` and `{scope:"process",providers:[...]}`, and reports gateway-local state only. Worker provider logs retain original request/job IDs. Circuit rejection returns `503 PROVIDER_CIRCUIT_OPEN` and `Retry-After`.
- The shared failure predicate keeps router fallback and job retry classification aligned. A circuit error's `retryAfterMs` flows through `JobProcessingError` to `PostgresJobStore.fail()`: `available_at` uses the greater of circuit delay and bounded exponential backoff. This avoids exhausting retries while waiting for cooldown. A routing pass skipped by circuits still counts as a job attempt.
- Job leases cover the sum of enabled provider timeouts plus 30s. Compose forwards identical routing settings to gateway and worker and defaults their shutdown grace to 180s. Increase grace for longer timeouts and account for queued HTTP work. No SQL migration was needed; retry scheduling SQL changed and its integration regression is pending successful database authentication.
- Small related fix: a successful Ollama response containing JSON `null` now raises the typed invalid-response error rather than a TypeError.

Async flow:

1. `POST /v1/jobs` authenticates and rate-limits the caller, validates the shared chat body and required `Idempotency-Key`, then calls `JobQueue.enqueue()`.
2. Postgres atomically stores the job and a pending-publication flag. The API returns `202`, `{ id, requestId }`, and a `Location` header.
3. A separate worker locks due rows with `FOR UPDATE SKIP LOCKED`, publishes persistent `{ id, attempt }` messages to RabbitMQ, waits for a publisher confirm, and commits the cleared publication flag.
4. A consumer atomically claims an attempt in Postgres, calls the broker-neutral inference processor, then saves the result or retry decision before acknowledging the delivery.
5. `GET /v1/jobs/:id` returns owner-scoped status and, when completed, a provider-neutral `response`. Another owner's job looks like a missing job (`404`).

States are `queued`, `active`, `retrying`, `completed`, and `failed`. Polling and duplicate submissions also consume rate-limit tokens. Reusing an idempotency key with identical input returns the original job and request ID; different input returns `409`. Object property order is ignored when hashing input; array order is preserved.

## Files to read and responsibilities

| File or group | Responsibility |
| --- | --- |
| `src/queue/job-queue.ts` | Generic `JobQueue<TData, TResult>` with `enqueue()` / `getJob()`, `JobProcessor`, and `JobWorker.run()` / `close()` |
| `src/queue/rabbitmq.ts` | Queue adapter, dispatch/maintenance loop, attempt processing, persistence-before-ack ordering |
| `src/queue/rabbitmq-broker.ts` | AMQP connection, topology, confirms, mandatory routing, prefetch, acknowledgements, shutdown |
| `src/queue/postgres-job-store.ts` | Idempotency, publication flag, atomic claims, results, retry scheduling, leases, retention |
| `src/queue/job-error.ts` | Broker-neutral processing errors and safe public error mapping |
| `src/providers/router.ts`, `circuit-breaker.ts`, `failure-policy.ts` | Primary/fallback selection, concurrency-safe circuit recovery, shared error classification |
| `src/providers/openai.ts`, `create-provider.ts` | Hosted adapter and shared runtime composition |
| `src/routes/providers.ts` | Protected process-local circuit snapshot endpoint |
| `src/jobs/inference-jobs.ts` | Chat-specific queue alias and queue name |
| `src/jobs/inference-processor.ts` | Provider call, structured logs, retryable/permanent error classification |
| `src/routes/jobs.ts` | Async submission and polling routes, depending on the generic queue contract |
| `src/routes/chat-schema.ts`, `src/routes/chat.ts` | Shared chat schemas and existing synchronous route |
| `src/app.ts`, `src/errors.ts` | Route registration and centralized application errors |
| `src/server.ts`, `src/worker.ts` | Runtime composition; gateway and separate worker entry points |
| `db/migrations/002_create_inference_jobs.sql` | Jobs table, constraints, and indexes |
| `src/scripts/migrate.ts` | Applies all sorted SQL migration files; current migrations are idempotent |
| `src/config.ts`, `.env.example` | RabbitMQ URL and job concurrency/attempt/backoff configuration |
| `docker-compose.yml` | Gateway, worker, RabbitMQ, Redis, Ollama; connects to host Postgres |
| `package.json`, `package-lock.json` | `amqplib` and types, worker and integration-test scripts; BullMQ removed |
| `test/jobs.test.ts`, `test/inference-processor.test.ts` | Mocked API and provider-processing tests |
| `test/circuit-breaker.test.ts`, `test/provider-routing.test.ts` | Deterministic recovery races, mapping/fallback policy, HTTP/worker integration, logs, retry delay |
| `test/openai.test.ts`, `test/create-provider.test.ts` | Hosted adapter validation/timeouts and real factory composition with mocked HTTP |
| `test/rabbitmq*.test.ts`, `test/postgres-job-store.test.ts`, `test/config.test.ts` | Broker/worker failure ordering, outbox confirmation, and configuration tests |
| `test/integration/` | Real Postgres job-store tests and RabbitMQ + Postgres tests with a stub provider |
| `README.md`, `AGENTS.md`, `TODO.md` | Setup and API examples, roadmap/preferences, deferred synchronous cancellation |

BullMQ source, tests, dependency, and stale compiled adapter files were removed. Redis is still required for rate limiting. Old Redis queue jobs are not automatically migrated.

## Decisions and limitations to preserve

- **Transactional publication:** the job row doubles as an outbox. If publication succeeds but its Postgres transaction does not commit, publication may repeat. The conditional attempt claim prevents duplicate deliveries from running the same attempt concurrently under normal operation.
- **Delivery is at least once:** a crash after inference but before persisting its result can repeat inference. Leases expire after the sum of enabled provider timeouts plus 30000ms; updates check the attempt number and active state so stale attempts cannot overwrite newer outcomes.
- **Retries:** default three total attempts, exponential delays starting at 1000 ms, capped at one hour. Provider connection failures, timeouts, HTTP 429, and 5xx retry. Open circuits also retry after at least the remaining cooldown (which can exceed the backoff cap). Other upstream 4xx, invalid responses, and unexpected application errors are permanent. Postgres stores when the next attempt is due; dispatch polls roughly every second.
- **Async capacity:** RabbitMQ single-active-consumer mode plus `JOB_CONCURRENCY` prefetch (default 2) bounds normal async execution. Additional workers are standby consumers and can also dispatch pending rows. This capacity is separate from synchronous gateway limits. Connection loss/lease recovery can temporarily overlap old and new execution.
- **Shutdown ordering matters:** drain active calls before cancelling the single active consumer, or a standby could start extra calls while the old worker is still running. Deliveries received during draining stay unacknowledged until connection close.
- **Infrastructure failures:** leave deliveries unacknowledged and exit the worker. Compose restarts failures; host workers need restarting. There is no in-process reconnection supervisor. Postgres result-write failures must not be reclassified as provider failures.
- **Broker topology:** durable quorum main queue `modelmux-inference` and dead-letter queue `modelmux-inference.failed`; persistent messages, confirms, mandatory routing, manual acknowledgements. Broker delivery limits are disabled because Postgres bounds application attempts. Local single-node RabbitMQ has no replica redundancy.
- **Dead letters:** malformed deliveries and permanent/exhausted processing failures are rejected. Postgres status is authoritative; a crash between recording failure and rejection, or lease exhaustion, can leave a failed job without a dead-letter copy.
- **Retention:** worker maintenance deletes terminal Postgres jobs after 24 hours; deduplication expires when the row is removed. Dead-letter messages have a 24-hour TTL. Pending backlog is unbounded in this milestone.
- **Availability:** the gateway writes jobs to Postgres without connecting to RabbitMQ, so `202` can succeed while the broker is down. Workers publish when available again.
- **Deferred:** synchronous client-abort propagation through the router and adapters remains in `TODO.md`. Streaming, caching, metrics, and load testing are later milestones.

## Verification at handoff

Current session verification (2026-09-29):

| Command | Result |
| --- | --- |
| `npm run check` | Passed: strict source/test typechecking and all 95 unit/injection tests |
| `npm run build` | Passed |
| `docker compose config --quiet` | Passed; validates configuration, not running services |
| `git diff --check` | Passed |
| `npm run test:store` | Sandbox initially denied sockets (`EPERM`); approved rerun reached local Postgres and failed authentication (`28P01`, user `modelmux`) before creating a test schema |
| `npm run test:integration` | Not rerun this session because the shared database prerequisite still fails; prior run also failed authentication |
| Live Ollama/OpenAI fallback | Not run; adapter/router tests use mocked HTTP and no paid requests were sent |

The user was previously asked to update `DATABASE_URL` in their local `.env` and replied “ok.” Authentication still failed on 2026-09-29. Do not interpret that reply as evidence credentials were fixed. Do not print credentials or overwrite `.env` with example defaults.

The failed integration run did not create its test schemas or reach RabbitMQ checks. Application migration `002` has not been verified as applied to the user's database. Actual RabbitMQ delivery and SQL behavior remain unverified against live services; mocked tests are not a substitute. Earlier Docker checks found the daemon unavailable, but service availability must be rechecked rather than assumed unchanged.

## How to resume

1. Read this handoff, inspect the current diff (including untracked files), and follow the user's latest request. Milestone 5 is implemented with passing unit/type checks; live database/broker/provider verification remains outstanding.
2. Once valid local Postgres credentials are configured, ensure RabbitMQ is available. Do not introduce a Postgres container as a workaround.
3. Run the integration tests below and fix any actual SQL, AMQP, retry, or shutdown failures. Record new results here. Tests need permission to create a schema in the configured database; they create and remove only random test schemas and queues, use a stub provider, and do not touch application tables.

```bash
docker compose up -d rabbitmq
npm run test:store
npm run test:integration
```

For an application smoke test with the gateway and worker on the host, preserve the existing `.env`, use the local `DATABASE_URL`, and follow the README's authenticated submission/polling examples:

```bash
docker compose up -d redis rabbitmq ollama
docker compose exec ollama ollama pull llama3.2
npm run db:migrate
# Only if a new API key is needed; save the printed key locally:
npm run api-key:create -- local-dev 10 1
npm run dev
```

In a second terminal:

```bash
npm run worker:dev
```

The migration command intentionally installs application tables; integration tests isolate their own tables without requiring that application migration. No live Ollama smoke test of the new async path has been completed in this pass.

After code changes, run `npm run check` and `npm run build`; validate Compose if edited. Do not mark live verification complete until it actually passes. Do not proceed to Milestone 6 without the user's request. Hosted fallback activation requires the user's own key and choice of hosted model; see README for the exact environment settings.
