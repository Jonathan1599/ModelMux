# ModelMux agent handoff

Last updated: 2026-10-06. This is a snapshot; inspect `git status` and the current files before continuing.

## Where we stopped

Milestones 1–7 are committed. HEAD at the start of the observability session was `a67cafe` (`feat: added cache for user chats`), following `a610afc` (SSE), and the worktree was clean. Milestone 4 live verification remains outstanding: local Postgres credentials were rejected on 2026-09-29 and have not been verified as fixed.

The user approved Milestone 8: catch up caching documentation, add gateway/worker Prometheus metrics, and provision one Grafana dashboard. Implementation is now in the working tree; local test and infrastructure verification status is below. No commit or Milestone 9 work is authorized. Hosted fallback remains opt-in; `.env` has not been edited and no hosted inference requests have been made.

Milestone 7 adds opt-in (`X-Cache-Mode: use`) Redis exact caching to ordinary chat only, per API-key owner, whole request and versioned routing configuration. Hits bypass provider capacity; cache failures fail open but auth/rate limiting remain mandatory. Atomic TTL/NX writes keep the first stored answer; there is no single-flight inference guarantee. See README for retention, privacy, fallback reuse, and shared Redis memory tradeoffs. Verification before this session: strict typechecking + 131 unit/API tests and 4 socket tests passed. Live `test:cache` could not connect even with socket permissions; Docker daemon checks also failed. No cache test key was created. Previously unfinished caching documentation is now brought current.

The module configuration remains **CommonJS**, with extensionless TypeScript imports (`f1d85fa`). Preserve it unless asked to change it.

## User constraints and preferences

- Work in this repository root. The project name is **ModelMux**; do not create a nested `llm-gateway` directory.
- Use the user's existing local Postgres. **Do not start, stop, replace, or manage Postgres through Docker.** Host processes use `DATABASE_URL`; Compose gateway/worker use `DOCKER_DATABASE_URL` with `host.docker.internal`.
- Keep implementations readable and suitable for explaining in interviews. Avoid frameworks or abstractions without a concrete need.
- Preserve the generic queue contract so another broker can replace RabbitMQ without rewriting routes or inference processing.
- Use `response` for Fastify handler variables, following the user's naming preference.
- Preserve all existing modified and untracked work. The backend diff is Milestone 8 on top of committed Milestones 1–7. An unrelated untracked `melissa-site/` directory appeared during the session; it was not created, inspected, or edited by this task.
- Do not implement later milestones or commit changes automatically. See `AGENTS.md` for the roadmap and working preferences.

## Implemented behavior

Synchronous chat remains `POST /v1/chat`: API-key authentication backed by Postgres, Redis token-bucket rate limiting, process-local FIFO concurrency control, and the `LLMProvider` abstraction. A provider router now calls Ollama and optionally OpenAI. `/health` only reports that the gateway process is up.

### Milestone 8 observability

- Separate Fastify metrics listeners default to gateway `127.0.0.1:9464` and worker `127.0.0.1:9465`. They require no API key or Redis token; public API `/metrics` remains a 404. Compose uses container-network binds with no published metrics ports. Bind failures warn without stopping inference.
- Each process has its own registry, labelled `service`. Typed best-effort metric events keep recording failures out of business control flow. Labels use bounded routes/methods/outcomes/providers, never prompts, model names, identities, raw URLs, or request/job IDs. Existing structured logs preserve request correlation.
- HTTP raw finish/close hooks include auth/validation failures and hijacked SSE, count once, and release active gauges on disconnect. Transport completion/status is separate from streaming handler success/failure/cancellation. Provider metrics surround actual routed calls; circuit skips are separate, and cancellation/consumer errors are not provider failures. Durations include whole streams and awaited downstream writes, not first-token latency.
- Gateway concurrency occupancy and admission wait/outcome metrics describe the process-local FIFO. Gateway-only `jobs_depth` reads shared Postgres nonterminal rows, not broker delivery count; aggregate replicated snapshots with `max`, not `sum`. One-second collection/driver waits bound scraping, while the existing ten-second statement timeout bounds SQL execution. Timeout does not itself cancel the query. Scrapes coalesce; failed collections emit `NaN` and collector health zero rather than misleading zero backlog.
- Worker metrics begin after a successful claim and end after result/retry persistence, before broker ack. Duplicates with no claim do not count as attempts. Retries count claimed attempt numbers greater than one; queue wait is database availability-to-claim time, excluding intentional backoff. Attempt outcomes are not unique terminal jobs; a completion update can be fenced to a no-op. Persistence failures remain infrastructure failures and leave deliveries unacknowledged.
- Optional Compose `observability` profile provisions Prometheus and one Grafana dashboard; UIs bind host loopback on 9090 and 3001. Prometheus retains seven days and scrapes every 15s. Dashboard covers HTTP p50/p95, rejection/occupancy/wait, provider errors/latency/routing/circuits, cache, SSE outcomes, durable backlog, and worker attempts/retries. No synthetic benchmark data is included. Low/no traffic can mean absent series; labelled counters appear on observation.
- Token usage/cost are explicitly deferred until the provider-neutral contract carries reliable metadata. `prom-client` 15 is used for the current Node 20 runtime; npm deprecates it in favor of `@prometheus-io/client`, whose current version requires newer Node. The warning was disclosed; a runtime/dependency migration is a follow-up, not silently included here.
- Docs cover metrics interpretation, privacy, provisioning, host-vs-Compose targeting, and verification limits. Live Prometheus/Grafana and new SQL remain unverified; see below.

### Milestone 6 streaming and cancellation behavior

- `POST /v1/chat/stream` uses the same body, authentication, per-request rate limiting, and concurrency pool as ordinary chat. It bypasses RabbitMQ; worker jobs stay non-streaming and independent of client disconnects.
- `LLMProvider.stream(request, onDelta, context)` awaits the provider-neutral text consumer, carrying socket backpressure upstream. Streaming is optional for custom providers; a missing implementation returns `501 STREAMING_UNSUPPORTED`.
- Ollama NDJSON and OpenAI Chat Completions SSE are decoded incrementally with split-UTF-8 handling, runtime validation, a 65,536-decoded-character line/data-event bound, and explicit terminal markers. The gateway does not accumulate the full answer; parser and transport buffers still exist. Official schemas were checked against provider docs linked in README. OpenAI requests keep `store:false` and redirect rejection; no SDK was added.
- SSE sends `delta` with `{content,requestId}`, `done` with `{requestId}`, or a sanitized `error` with `{code,message,requestId}`. Headers preserve request/rate-limit fields and disable caching/proxy buffering where supported. Before first text, failures use normal JSON HTTP errors. After output, errors retain HTTP 200 and terminate without `done`; an unwritable socket may simply close. Clients must require `done`, not infer success from EOF.
- Router fallback is allowed only before the first nonempty delta is handed to the consumer. A circuit surrounds the full stream, not just its HTTP headers. Client cancellation and consumer errors do not alter provider health or trigger fallback. An ignored half-open outcome makes a new recovery probe immediately eligible while preserving the failure count.
- `requestCancellation()` listens to incoming abort, response close/error, and gateway shutdown. It deliberately does not use incoming `close`, which also fires after normal request-body completion. Both chat routes cancel queued/active work. `acquire(signal)` removes cancelled waiters and clears timers/listeners; routes check again after admission and release every acquired permit in `finally`. Snapshots count aborted acquisitions, and logs distinguish queue vs execution cancellation.
- Whole-call provider deadlines cover HTTP, body parsing, and awaited consumer callbacks. A full socket buffer pauses upstream consumption until `drain`; a 15s stalled-write deadline cancels the request. The earlier deadline wins. If the overall provider deadline expires while downstream is slow, it still counts as a provider timeout; this tradeoff is documented rather than claiming precise attribution. A pending full-buffer write is never followed by another queued error event.
- Fastify `preClose` cancels active and queued synchronous chat before closing connections. A shared shutdown signal has an explicit unlimited listener cap because every in-flight request subscribes; each listener is removed in `finally`. Worker shutdown still drains accepted jobs normally.
- Limits: no stream persistence/resume, heartbeat, tool-call forwarding, token usage, or public finish-reason field. `done` confirms normal protocol termination, not a natural model stop (length-limited output can also end normally). Upstream transport abort does not prove remote GPU work stops immediately.

### Milestone 5 routing behavior

- `src/providers/create-provider.ts` composes the same router in gateway and worker; every process has independent provider circuit state. The HTTP permit covers the full routing pass, while worker prefetch bounds async passes.
- Primary Ollama is called once per pass. Connection failures, timeouts, upstream `429`/`5xx`, or an open circuit permit one fallback call. Other upstream `4xx`, malformed responses, and unexpected application errors do not trigger fallback. The final target's error determines HTTP/job failure classification; a permanent hosted error ends an async attempt without retry even if the primary error was transient.
- `OPENAI_FALLBACK_MODEL_MAP` is an explicit JSON mapping of incoming local model names to hosted replacements. Missing/blank/empty mappings disable hosted fallback, even if an API key exists. A non-empty map requires `OPENAI_API_KEY`. Only own, exact matching map entries permit forwarding. Credentials and the map are not exposed by provider status.
- `OpenAIProvider` uses native fetch against the fixed HTTPS Chat Completions endpoint, blocks redirects, sends `store:false` and selects `stream` by route, and performs no internal retries. It validates the response at runtime; a textual refusal is a completed response. Hosted HTTP error bodies and raw causes are discarded. Official request/response documentation: https://developers.openai.com/api/reference/resources/chat/subresources/completions/methods/create . No SDK dependency was added.
- Circuits default to five consecutive transient failures and a 30s cooldown. A single real request probes after cooldown; concurrent callers skip that provider. Success or a non-transient error resets/closes the circuit. Generations prevent late outcomes from old calls from changing newer state. Snapshots do not advance the circuit or probe providers. Circuits are shared across that provider's models and reset on restart.
- `GET /v1/providers` is authenticated and rate-limited, returns `Cache-Control:no-store` and `{scope:"process",providers:[...]}`, and reports gateway-local state only. Worker provider logs retain original request/job IDs. Circuit rejection returns `503 PROVIDER_CIRCUIT_OPEN` and `Retry-After`.
- The shared failure predicate keeps router fallback and job retry classification aligned. A circuit error's `retryAfterMs` flows through `JobProcessingError` to `PostgresJobStore.fail()`: `available_at` uses the greater of circuit delay and bounded exponential backoff. This avoids exhausting retries while waiting for cooldown. A routing pass skipped by circuits still counts as a job attempt.
- Job leases cover the sum of enabled provider timeouts plus 30s. Compose forwards identical routing settings to gateway and worker and defaults their shutdown grace to 180s. Increase grace for longer worker drain times; synchronous gateway chat is now cancelled on shutdown. No SQL migration was needed; retry scheduling SQL changed in Milestone 5 and its integration regression is pending successful database authentication.
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
| `src/observability/telemetry.ts`, `metrics.ts` | Best-effort typed events, per-process registries, low-cardinality metric definitions and snapshot collection |
| `src/observability/http.ts`, `server.ts` | HTTP transport lifecycle hooks and independent internal scrape listener |
| `observability/` | Prometheus scrape config, Grafana datasource/dashboard provisioning, and overview dashboard |
| `src/cache/` | Owner-scoped exact-cache identity, eligibility contract, and bounded Redis operations |
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
| `src/routes/chat-schema.ts`, `src/routes/chat.ts` | Shared chat schemas, synchronous chat, and direct SSE route |
| `src/routes/request-cancellation.ts`, `src/routes/sse.ts` | Disconnect/shutdown signal lifecycle; SSE framing and bounded backpressure |
| `src/cancellation.ts`, `src/providers/http-request.ts`, `stream-parser.ts` | Shared cancellation error, whole-call deadlines, incremental upstream decoding |
| `src/app.ts`, `src/errors.ts` | Route registration and centralized application errors |
| `src/server.ts`, `src/worker.ts` | Runtime composition; gateway and separate worker entry points |
| `db/migrations/002_create_inference_jobs.sql` | Jobs table, constraints, and indexes |
| `src/scripts/migrate.ts` | Applies all sorted SQL migration files; current migrations are idempotent |
| `src/config.ts`, `.env.example` | RabbitMQ URL and job concurrency/attempt/backoff configuration |
| `docker-compose.yml` | Gateway, worker, RabbitMQ, Redis, Ollama; optional monitoring profile; connects to host Postgres |
| `package.json`, `package-lock.json` | Runtime dependencies including `amqplib`/`prom-client`, worker and verification scripts |
| `test/jobs.test.ts`, `test/inference-processor.test.ts` | Mocked API and provider-processing tests |
| `test/circuit-breaker.test.ts`, `test/provider-routing.test.ts` | Deterministic recovery races, mapping/fallback policy, HTTP/worker integration, logs, retry delay |
| `test/openai.test.ts`, `test/create-provider.test.ts` | Hosted adapter validation/timeouts and real factory composition with mocked HTTP |
| `test/provider-streaming.test.ts`, `test/sse.test.ts`, `test/concurrency.test.ts` | Stream parsing, deadlines, backpressure, fallback/circuit cancellation, FIFO aborts |
| `test/http/streaming.test.ts`, `test/helpers/chat-app.ts` | Localhost HTTP streaming, disconnect, and shutdown tests with stub dependencies |
| `test/metrics.test.ts`, `test/http/metrics.test.ts`, `test/observability-config.test.ts` | Metric values/privacy/failure isolation, real HTTP scrape boundary, static dashboard/config contracts |
| `test/*cache*.test.ts`, `test/cache/` | Mocked cache semantics and separately invoked live Redis verification |
| `test/rabbitmq*.test.ts`, `test/postgres-job-store.test.ts`, `test/config.test.ts` | Broker/worker failure ordering, outbox confirmation, and configuration tests |
| `test/integration/` | Real Postgres job-store tests and RabbitMQ + Postgres tests with a stub provider |
| `README.md`, `AGENTS.md`, `TODO.md` | Setup and streaming/API examples, roadmap/preferences, cancellation status and verification follow-ups |

BullMQ source, tests, dependency, and stale compiled adapter files were removed. Redis is still required for rate limiting and also serves optional exact caching. Old Redis queue jobs are not automatically migrated.

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
- **Deferred:** load testing, final polish, token usage/cost metadata, and optional semantic-cache experiments. Exact caching, metrics, and streaming/cancellation are implemented; remaining live verification and deliberate limitations are recorded in `TODO.md`.

## Verification at handoff

Current session verification (2026-10-06):

| Command | Result |
| --- | --- |
| `npm run check` | Passed: strict source/test typechecking and all 144 unit/injection tests, including static dashboard/config contracts |
| `npm run test:streaming` | Passed all 4 real localhost HTTP tests with socket permissions, including disconnect metrics and shutdown with 12 queued requests |
| `npm run test:metrics` | Passed 1 real localhost HTTP test with socket permissions: separate unauthenticated scrape endpoint, API rate-limit isolation, correct completion count |
| `npm run build` | Passed |
| `docker compose --profile observability config --quiet` | Passed; does not validate live Prometheus/Grafana rendering |
| `docker compose --profile observability ps` | Approved daemon access still failed: cannot connect to `/var/run/docker.sock`. Live monitoring unavailable |
| `git diff --check` | Passed |
| `npm run test:cache` | Not rerun in Milestone 8; previous approved attempt could not connect to Redis, created no test key |
| `npm run test:store` | Not rerun. Previous approved run (2026-09-29) failed local Postgres authentication (`28P01`, user `modelmux`) before creating a test schema |
| `npm run test:integration` | Not rerun this session; previous runs failed the shared database prerequisite |
| Live Ollama/OpenAI streaming/fallback | Not run; mocked protocol tests and a real localhost stub upstream only. No paid requests were sent |

The user was previously asked to update `DATABASE_URL` in their local `.env` and replied “ok.” Authentication still failed on 2026-09-29. Do not interpret that reply as evidence credentials were fixed. Do not print credentials or overwrite `.env` with example defaults.

The failed integration run did not create its test schemas or reach RabbitMQ checks. Application migration `002` has not been verified as applied to the user's database. Actual RabbitMQ delivery and SQL behavior remain unverified against live services; mocked tests are not a substitute. Earlier Docker checks found the daemon unavailable, but service availability must be rechecked rather than assumed unchanged.

## How to resume

1. Read this handoff, inspect the current backend diff (including untracked observability files), and follow the user's latest request. Milestone 8 implementation is in the working tree; live infrastructure/provider verification remains outstanding. Preserve unrelated user work. Do not start Milestone 9 or commit automatically.
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

After code changes, run `npm run check`, `npm run test:streaming`, `npm run test:metrics`, and `npm run build`; validate Compose if edited. Run `npm run test:cache` once Redis is available. Use README's `curl -N` example for a live streaming smoke test once auth dependencies work. For monitoring, start the README's optional Compose profile and check both Prometheus targets, collector health, and the Grafana dashboard; static tests do not parse PromQL or prove live rendering. Do not mark live verification complete until it actually passes. Hosted fallback activation requires the user's own key and choice of hosted model; see README for exact environment settings.
