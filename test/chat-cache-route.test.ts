import assert from "node:assert/strict";
import test from "node:test";
import { buildApp } from "../src/app";
import type { ChatCache } from "../src/cache/chat-cache";
import { chatCacheKey } from "../src/cache/cache-key";
import { abortable, RequestAbortedError } from "../src/cancellation";
import { InMemoryConcurrencyLimiter } from "../src/concurrency/in-memory-concurrency-limiter";
import { AuthenticationError, ProviderConnectionError } from "../src/errors";
import { CircuitBreaker } from "../src/providers/circuit-breaker";
import type { LLMProvider } from "../src/providers/provider";
import { ProviderRouter } from "../src/providers/router";
import type { ChatRequest, ChatResponse } from "../src/types/chat";
import { Metrics } from "../src/observability/metrics";
import { metricSample as sample } from "./helpers/metric-sample";

const body: ChatRequest = { model: "local", messages: [{ role: "user", content: "Hello" }] };
const answer: ChatResponse = { message: { role: "assistant", content: "answer" } };
const headers = { authorization: "Bearer alice", "x-cache-mode": "use" };

function fixture(cacheEnabled = true, overrideProvider?: LLMProvider) {
  const metrics = new Metrics("gateway");
  const entries = new Map<string, ChatResponse>();
  const calls = { reads: 0, writes: 0, provider: 0, rateLimit: 0, streams: 0, jobs: 0 };
  const access = { authenticated: true, allowed: true };
  const cache: ChatCache = {
    async get(owner, request) {
      calls.reads += 1;
      return entries.get(chatCacheKey("test", owner, request)) ?? null;
    },
    async put(owner, request, value) {
      calls.writes += 1;
      entries.set(chatCacheKey("test", owner, request), value);
    },
  };
  const provider: LLMProvider = overrideProvider ?? {
    async chat() { calls.provider += 1; return answer; },
    async stream(_request, emit) { calls.streams += 1; await emit("live text"); },
  };
  const limiter = new InMemoryConcurrencyLimiter({ maxConcurrent: 1, maxQueueSize: 0, waitTimeoutMs: 100 });
  const app = buildApp({
    provider, chatCache: cacheEnabled ? cache : undefined, concurrencyLimiter: limiter, logger: false, metrics,
    apiKeyAuthenticator: { async authenticate(header) {
      if (!access.authenticated || !["Bearer alice", "Bearer bob"].includes(header ?? "")) throw new AuthenticationError();
      return { id: header!, name: "test", keyPrefix: "test", rateLimit: { capacity: 10, refillPerSecond: 1 } };
    } },
    rateLimiter: { async consume() {
      calls.rateLimit += 1;
      return { allowed: access.allowed, remaining: 10 - calls.rateLimit, retryAfterSeconds: 1 };
    } },
    jobs: {
      async enqueue() { calls.jobs += 1; return { id: "job", requestId: "job-request" }; },
      async getJob() { throw new Error("unused"); },
    },
  });
  const send = (extraHeaders: Record<string, string> = {}, payload: unknown = body) => app.inject({
    method: "POST", url: "/v1/chat", headers: { ...headers, ...extraHeaders }, payload: payload as object,
  });
  return { app, cache, entries, calls, access, limiter, send, metrics };
}

test("cache hit skips provider capacity but still authenticates, limits and supplies fresh request headers", async (t) => {
  const { app, send, calls, limiter, metrics } = fixture();
  t.after(() => app.close());
  const first = await send({ "x-request-id": "first" });
  assert.equal(first.headers["x-cache"], "MISS");
  assert.equal(first.statusCode, 200);
  const occupied = await limiter.acquire();
  t.after(occupied.release);
  const hit = await send({ "x-request-id": "second" });
  assert.equal(hit.statusCode, 200);
  assert.deepEqual(hit.json(), answer);
  assert.equal(hit.headers["x-cache"], "HIT");
  assert.equal(hit.headers["x-request-id"], "second");
  assert.equal(hit.headers["x-ratelimit-remaining"], "8");
  assert.equal(hit.headers["cache-control"], "no-store");
  assert.equal(calls.provider, 1);
  assert.equal(calls.rateLimit, 2);
  assert.equal(calls.writes, 1);
  assert.equal(await sample(metrics, "cache_lookups_total", { status: "HIT" }), 1);
  assert.equal(await sample(metrics, "cache_lookups_total", { status: "MISS" }), 1);
  assert.equal(limiter.snapshot().active, 1);
  assert.equal(limiter.snapshot().rejected, 0);
  occupied.release();
});

test("the cache is opt-in, respects bypass/no-store/no-cache, and never crosses API-key owners", async (t) => {
  const { app, send, calls } = fixture();
  t.after(() => app.close());
  for (const selectedHeaders of [
    { authorization: "Bearer alice" }, { ...headers, "x-cache-mode": "bypass" },
    { ...headers, "cache-control": "private, No-Store" }, { ...headers, "cache-control": "no-cache" },
  ]) {
    const response = await app.inject({ method: "POST", url: "/v1/chat", headers: selectedHeaders, payload: body });
    assert.equal(response.statusCode, 200);
    assert.equal(response.headers["x-cache"], "BYPASS");
  }
  assert.equal(calls.reads, 0);
  assert.equal(calls.writes, 0);
  assert.equal((await send()).headers["x-cache"], "MISS");
  assert.equal((await send()).headers["x-cache"], "HIT");
  assert.equal((await send({ authorization: "Bearer bob" })).headers["x-cache"], "MISS");
  assert.equal((await send({ authorization: "Bearer bob" })).headers["x-cache"], "HIT");
  assert.equal(calls.provider, 6);
});

test("cache hits cannot bypass revoked auth, exhausted rate limits, or validation", async (t) => {
  const { app, send, access, calls } = fixture();
  t.after(() => app.close());
  await send();
  access.authenticated = false;
  assert.equal((await send()).statusCode, 401);
  access.authenticated = true;
  access.allowed = false;
  assert.equal((await send()).statusCode, 429);
  access.allowed = true;
  assert.equal((await send({}, {})).statusCode, 400);
  assert.equal((await send({ "x-cache-mode": "invalid" })).statusCode, 400);
  assert.equal(calls.reads, 1);
  assert.equal(calls.provider, 1);
});

test("missing cache configuration bypasses, and SSE/jobs never read or write the chat cache", async (t) => {
  const disabled = fixture(false);
  t.after(() => disabled.app.close());
  assert.equal((await disabled.send()).headers["x-cache"], "BYPASS");
  assert.equal(disabled.calls.reads, 0);
  const { app, calls, send } = fixture();
  t.after(() => app.close());
  await send();
  const streamed = await app.inject({ method: "POST", url: "/v1/chat/stream", headers, payload: body });
  assert.match(streamed.payload, /live text/);
  assert.equal(calls.streams, 1);
  assert.equal(streamed.headers["x-cache"], undefined);
  const job = await app.inject({ method: "POST", url: "/v1/jobs", headers: { ...headers, "idempotency-key": "key" }, payload: body });
  assert.equal(job.statusCode, 202);
  assert.equal(calls.jobs, 1);
  assert.equal(calls.reads, 1);
  assert.equal(calls.writes, 1);
});

test("cache read/write failures preserve normal inference and never expose cache errors", async (t) => {
  for (const operation of ["get", "put"] as const) {
    const { app, send, calls, cache, limiter } = fixture();
    t.after(() => app.close());
    t.mock.method(cache, operation, async () => { throw new Error("secret prompt and credentials"); });
    const response = await send();
    assert.equal(response.statusCode, 200);
    assert.deepEqual(response.json(), answer);
    assert.equal(response.headers["x-cache"], operation === "get" ? "BYPASS" : "MISS");
    assert.equal(calls.provider, 1);
    assert.equal(calls.writes, 0);
    assert.equal(limiter.snapshot().active, 0);
    assert.doesNotMatch(response.payload, /secret|credentials/);
  }
});

test("provider failures are never cached and retain their normal status", async (t) => {
  const { app, send, calls, limiter } = fixture(true, {
    async chat() { throw new ProviderConnectionError("Ollama"); },
  });
  t.after(() => app.close());
  assert.equal((await send()).statusCode, 503);
  assert.equal((await send()).statusCode, 503);
  assert.equal(calls.writes, 0);
  assert.equal(limiter.snapshot().active, 0);
});

test("cache writes happen after releasing provider capacity", async (t) => {
  const { app, send, cache, limiter } = fixture();
  t.after(() => app.close());
  t.mock.method(cache, "put", async () => {
    assert.equal(limiter.snapshot().active, 0);
    const next = await limiter.acquire();
    next.release();
  });
  assert.equal((await send()).statusCode, 200);
});

test("cache hits bypass routing without resetting an open circuit or probing it", async (t) => {
  const breaker = new CircuitBreaker({ failureThreshold: 1, resetTimeoutMs: 1000 });
  let primaryCalls = 0;
  const router = new ProviderRouter({ name: "primary", circuit: breaker, provider: {
    async chat() { primaryCalls += 1; return answer; },
  } });
  const { app, send } = fixture(true, router);
  t.after(() => app.close());
  assert.equal((await send()).headers["x-cache"], "MISS");
  await assert.rejects(breaker.execute(async () => { throw new Error("outage"); }, () => true));
  assert.equal((await send()).headers["x-cache"], "HIT");
  assert.equal(breaker.snapshot().state, "open");
  assert.equal(primaryCalls, 1);
});

test("a completed fallback answer is reused under unchanged routing configuration", async (t) => {
  let fallbacks = 0;
  const router = new ProviderRouter({ name: "primary", circuit: new CircuitBreaker({ failureThreshold: 5, resetTimeoutMs: 100 }), provider: {
    async chat() { throw new ProviderConnectionError("primary"); },
  } }, { name: "fallback", modelMap: { local: "hosted" }, circuit: new CircuitBreaker({ failureThreshold: 5, resetTimeoutMs: 100 }), provider: {
    async chat(request) { assert.equal(request.model, "hosted"); fallbacks += 1; return answer; },
  } });
  const { app, send, calls } = fixture(true, router);
  t.after(() => app.close());
  assert.equal((await send()).headers["x-cache"], "MISS");
  assert.equal((await send()).headers["x-cache"], "HIT");
  assert.equal(fallbacks, 1);
  assert.equal(calls.writes, 1);
});

test("shutdown cancels pending cache I/O instead of starting or leaking inference", { timeout: 3000 }, async (t) => {
  for (const operation of ["get", "put"] as const) {
    const { app, send, cache, calls, limiter } = fixture();
    t.after(() => app.close());
    let started!: () => void;
    const ready = new Promise<void>((resolve) => { started = resolve; });
    let aborted = false;
    const block = async (signal: AbortSignal): Promise<never> => {
      started();
      try { return await abortable(new Promise<never>(() => {}), signal); }
      catch (error) { aborted = true; assert.ok(error instanceof RequestAbortedError); throw error; }
    };
    if (operation === "get") {
      t.mock.method(cache, "get", async (_owner: string, _request: ChatRequest, signal: AbortSignal) => block(signal));
    } else {
      t.mock.method(cache, "put", async (_owner: string, _request: ChatRequest, _response: ChatResponse, signal: AbortSignal) => block(signal));
    }
    // An intentionally destroyed response can reject injection.
    const pending = send().catch(() => undefined);
    await ready;
    await app.close();
    await pending;
    assert.equal(aborted, true);
    assert.equal(limiter.snapshot().active, 0);
    assert.equal(calls.provider, operation === "get" ? 0 : 1);
  }
});
