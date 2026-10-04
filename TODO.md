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
- Token usage and finish reasons are not exposed in streaming events yet. Track these deliberately with the observability milestone rather than assuming every `done` means the model reached a natural stop.
- Milestone 7 (exact caching) and later milestones require a new user request.
