import assert from "node:assert/strict";
import test from "node:test";
import { RequestAbortedError } from "../src/cancellation";
import { InMemoryConcurrencyLimiter } from "../src/concurrency/in-memory-concurrency-limiter";
import { ProviderConnectionError, ProviderCircuitOpenError } from "../src/errors";
import { Metrics } from "../src/observability/metrics";
import { buildMetricsServer } from "../src/observability/server";
import type { MetricSink } from "../src/observability/telemetry";
import { CircuitBreaker } from "../src/providers/circuit-breaker";
import { ProviderRouter } from "../src/providers/router";
import { chatApp, chatHeaders, chatPayload } from "./helpers/chat-app";
import { metricSample as sample } from "./helpers/metric-sample";
import type { ChatRequest } from "../src/types/chat";

const request: ChatRequest = { model: "sensitive-model-name", messages: [{ role: "user", content: "private prompt" }] };
const answer = { message: { role: "assistant" as const, content: "private answer" } };
const breaker = () => new CircuitBreaker({ failureThreshold: 1, resetTimeoutMs: 1000 });

test("HTTP metrics count auth/validation/rate errors once and use bounded route labels", async (t) => {
  const metrics = new Metrics("gateway");
  const { app, limitRequests } = chatApp({ async chat() { return answer; } }, 2, metrics);
  t.after(() => app.close());
  const inject = (headers = chatHeaders, payload: object = chatPayload) => app.inject({ method: "POST", url: "/v1/chat", headers, payload });
  await inject();
  await inject({ ...chatHeaders, authorization: "bad" });
  await inject(chatHeaders, {});
  await app.inject({ url: "/secret-job-123?prompt=never-label-this" });
  await app.inject({ url: "/another-secret-path" });
  limitRequests();
  await inject();
  for (const status of ["200", "401", "400", "429"]) {
    assert.equal(await sample(metrics, "http_requests_total", { route: "/v1/chat", status, outcome: "completed" }), 1);
  }
  assert.equal(await sample(metrics, "http_requests_total", { route: "unmatched", status: "404" }), 2);
  assert.equal(await sample(metrics, "rate_limit_rejections_total"), 1);
  assert.equal(await sample(metrics, "http_active"), 0);
  const text = await metrics.scrape();
  assert.doesNotMatch(text, /secret-job|never-label|another-secret|private prompt|private answer|stream-request|Bearer/);
  assert.match(text, /service="gateway"/);
});

test("SSE semantic failures are separate from completed HTTP 200 transports", async (t) => {
  const metrics = new Metrics("gateway");
  let failing = false;
  const { app } = chatApp({
    async chat() { return answer; },
    async stream(_request, emit) { await emit("partial"); if (failing) throw new ProviderConnectionError("ollama"); },
  }, 2, metrics);
  t.after(() => app.close());
  const send = () => app.inject({ method: "POST", url: "/v1/chat/stream", headers: chatHeaders, payload: chatPayload });
  assert.match((await send()).payload, /event: done/);
  failing = true;
  assert.match((await send()).payload, /event: error/);
  assert.equal(await sample(metrics, "streams_total", { outcome: "completed" }), 1);
  assert.equal(await sample(metrics, "streams_total", { outcome: "failed" }), 1);
  assert.equal(await sample(metrics, "http_requests_total", { route: "/v1/chat/stream", status: "200" }), 2);
  assert.equal(await sample(metrics, "http_active"), 0);
});

test("provider metrics distinguish calls, fallbacks, skips, and both process registries", async () => {
  const metrics = new Metrics("gateway");
  const router = new ProviderRouter({ name: "ollama", circuit: breaker(), provider: {
    async chat() { throw new ProviderConnectionError("ollama"); },
  } }, { name: "openai", circuit: breaker(), modelMap: { "sensitive-model-name": "hosted-private" }, provider: {
    async chat() { return answer; },
  } }, metrics);
  metrics.bindProviders(() => router.snapshot());
  await router.chat(request);
  await router.chat(request); // Primary is skipped, not timed as an actual call.
  assert.equal(await sample(metrics, "provider_calls_total", { provider: "ollama", outcome: "transient_error" }), 1);
  assert.equal(await sample(metrics, "provider_calls_total", { provider: "openai", outcome: "success" }), 2);
  assert.equal(await sample(metrics, "circuit_rejections_total", { provider: "ollama" }), 1);
  assert.equal(await sample(metrics, "fallbacks_total", { from: "ollama", to: "openai" }), 2);
  assert.equal(await sample(metrics, "provider_active", { provider: "openai", mode: "chat" }), 0);
  const text = await metrics.scrape();
  assert.equal(await sample(metrics, "circuit_state", { provider: "ollama", state: "open" }), 1);
  assert.doesNotMatch(text, /sensitive-model-name|hosted-private|private prompt|private answer/);
  const worker = new Metrics("worker");
  assert.equal(await sample(worker, "fallbacks_total", { from: "ollama", to: "openai" }), 0);
  assert.doesNotMatch(await worker.scrape(), /service="gateway"/);
});

test("cancelled provider streams and failed downstream consumers are not provider-error metrics", async () => {
  const metrics = new Metrics("gateway");
  const controller = new AbortController();
  const router = new ProviderRouter({ name: "ollama", circuit: breaker(), provider: {
    async chat() { return answer; }, async stream(_data, emit) { await emit("text"); },
  } }, undefined, metrics);
  await assert.rejects(router.stream(request, async () => { controller.abort(); }, { signal: controller.signal }), RequestAbortedError);
  await assert.rejects(router.stream(request, async () => { throw new Error("broken socket"); }));
  assert.equal(await sample(metrics, "provider_calls_total", { provider: "ollama", mode: "stream", outcome: "cancelled" }), 1);
  assert.equal(await sample(metrics, "provider_calls_total", { provider: "ollama", mode: "stream", outcome: "consumer_error" }), 1);
  assert.equal(await sample(metrics, "provider_active", { provider: "ollama", mode: "stream" }), 0);
});

test("concurrency observations distinguish full, timed-out and cancelled queues and scrape live occupancy", async () => {
  const metrics = new Metrics("gateway");
  const limiter = new InMemoryConcurrencyLimiter({ maxConcurrent: 1, maxQueueSize: 1, waitTimeoutMs: 10 }, metrics);
  metrics.bindConcurrency(() => limiter.snapshot());
  const active = await limiter.acquire();
  const controller = new AbortController();
  const cancelled = assert.rejects(limiter.acquire(controller.signal), RequestAbortedError);
  await assert.rejects(limiter.acquire());
  await metrics.scrape();
  assert.equal(await sample(metrics, "concurrency_active"), 1);
  assert.equal(await sample(metrics, "concurrency_queued"), 1);
  controller.abort();
  await cancelled;
  await assert.rejects(limiter.acquire()); // Queue wait timeout.
  active.release();
  await metrics.scrape();
  assert.equal(await sample(metrics, "concurrency_active"), 0);
  assert.equal(await sample(metrics, "concurrency_queued"), 0);
  for (const outcome of ["admitted", "queue_full", "cancelled", "timeout"]) {
    assert.equal(await sample(metrics, "admissions_total", { outcome }), 1);
    assert.equal(await sample(metrics, "queue_wait_seconds", { outcome }, "_count"), 1);
  }
});

test("scrapes coalesce, failed job collectors expose NaN, and recovery replaces failed samples", async () => {
  const metrics = new Metrics("gateway");
  let calls = 0;
  let healthy = false;
  metrics.bindQueueDepth(async () => {
    calls += 1;
    if (!healthy) return new Promise<never>(() => {});
    return { queued: 3, retrying: 2, active: 1 };
  }, 5);
  const [a, b] = await Promise.all([metrics.scrape(), metrics.scrape()]);
  assert.equal(a, b);
  assert.equal(calls, 1);
  assert.ok(Number.isNaN(await sample(metrics, "jobs_depth", { status: "queued" })));
  assert.equal(await sample(metrics, "collector_up", { collector: "jobs" }), 0);
  healthy = true;
  await metrics.scrape();
  assert.equal(await sample(metrics, "jobs_depth", { status: "queued" }), 3);
  assert.equal(await sample(metrics, "collector_up", { collector: "jobs" }), 1);
});

test("metrics endpoint has no auth dependency and sanitizes rendering failures", async (t) => {
  const metrics = new Metrics("gateway");
  const app = buildMetricsServer(metrics);
  t.after(() => app.close());
  const result = await app.inject({ url: "/metrics" });
  assert.equal(result.statusCode, 200);
  assert.match(result.headers["content-type"]!, /text\/plain/);
  assert.equal(result.headers["cache-control"], "no-store");
  t.mock.method(metrics.registry, "metrics", async () => { throw new Error("secret credentials"); });
  const failed = await app.inject({ url: "/metrics" });
  assert.equal(failed.statusCode, 503);
  assert.doesNotMatch(failed.payload, /secret credentials/);
});

test("throwing instrumentation cannot alter concurrency or routing outcomes", async () => {
  const broken: MetricSink = { record() { throw new Error("metrics unavailable"); } };
  const limiter = new InMemoryConcurrencyLimiter({ maxConcurrent: 1, maxQueueSize: 0, waitTimeoutMs: 1 }, broken);
  const permit = await limiter.acquire();
  permit.release();
  assert.equal(limiter.snapshot().active, 0);
  const router = new ProviderRouter({ name: "ollama", circuit: breaker(), provider: {
    async chat() { throw new ProviderConnectionError("ollama"); },
  } }, undefined, broken);
  await assert.rejects(router.chat(request), ProviderConnectionError);
  await assert.rejects(router.chat(request), ProviderCircuitOpenError);
});
