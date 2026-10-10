# TODO

## Completed in Milestone 6: chat cancellation and streaming

Both `POST /v1/chat` and `POST /v1/chat/stream` now propagate disconnect and gateway-shutdown cancellation through the FIFO limiter, provider router, and both HTTP adapters.

- Aborted queued requests are removed immediately and their timer/listener is cleared.
- A cancellation check immediately after admission prevents a disconnected request from starting inference.
- Active upstream fetch/body reads are aborted; every acquired permit is released in `finally`.
- Client cancellation does not trigger fallback, reset consecutive failures, or count as a provider failure. Cancelled half-open probes release the probe slot for a future request.
- Limiter snapshots count aborted acquisitions separately from timed-out waits. Route logs distinguish queue/execution cancellation.
- SSE has bounded drain waits and incremental parsing, and fallback is prohibited after the first text delta.
- Gateway shutdown cancels active and queued synchronous requests. Accepted async jobs are still durable and independent of the submitting HTTP connection; worker shutdown still drains them.

`npm run check` covers FIFO abort races, idempotent release, adapter cancellation, parsing, backpressure, and circuit/fallback behavior. `npm run test:streaming` uses real localhost sockets with a stub upstream to verify incremental delivery, queued/active disconnects, subsequent capacity reuse, and gateway shutdown.

## Remaining verification and deliberate limits

- Live Ollama streaming and optional hosted fallback still need an application smoke test with working local auth/rate-limit infrastructure. See `HANDOFF.md` for the previously observed Postgres authentication blocker; do not substitute a Docker Postgres instance.
- Verify proxy buffering/idle timeouts in the eventual deployment. There are no SSE heartbeats, replay, or resume semantics; do not add them without a concrete requirement.
- Disconnecting HTTP is best-effort upstream cancellation, not proof that remote inference stopped computing.
- Whole-call provider deadlines include downstream waits and still count as provider timeouts if they expire first. More nuanced slow-client attribution can be revisited with actual measurements.
- Token usage and finish reasons are not exposed in streaming events yet. Observability deliberately omits token counts and estimated cost until the provider contract carries reliable usage metadata. Do not assume every `done` means the model reached a natural stop.
- Exact caching is committed. Live Redis verification remains outstanding (connection failed; Docker daemon unavailable at the last check). See README for the opt-in header and cache privacy/retention tradeoffs.
- Milestone 8 observability is implemented. Live Prometheus scraping/Grafana rendering remain unverified because Docker was unavailable; static dashboard references and real localhost scrape tests are covered. Verify the new Postgres queue-depth/claim-wait SQL once local database authentication works.
- `prom-client` 15 supports the current Node 20 runtime but is deprecated by its maintainers. Plan an explicit runtime upgrade and migration to `@prometheus-io/client` (current replacement requires newer Node); do not silently raise the runtime requirement during an unrelated milestone.
- Monitor the scrape query's database cost under load. Durable backlog snapshots are shared across gateways, not additive or equal to RabbitMQ delivery counts. A one-second collector/driver timeout bounds waiting, not database execution; the existing statement timeout remains the SQL bound.
- Milestone 9 load testing requires a new user request. No performance figures have been claimed from unit tests or the provisioned dashboard.
