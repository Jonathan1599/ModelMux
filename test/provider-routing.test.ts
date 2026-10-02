import assert from "node:assert/strict";
import test from "node:test";
import pino from "pino";
import { buildApp, type BuildAppOptions } from "../src/app";
import type { ApiKeyPrincipal } from "../src/auth/api-keys";
import { InMemoryConcurrencyLimiter } from "../src/concurrency/in-memory-concurrency-limiter";
import {
  AuthenticationError,
  ProviderCircuitOpenError,
  ProviderConnectionError,
  ProviderHttpError,
  ProviderResponseError,
  ProviderTimeoutError,
} from "../src/errors";
import { createInferenceProcessor } from "../src/jobs/inference-processor";
import { CircuitBreaker } from "../src/providers/circuit-breaker";
import type { LLMProvider, ProviderContext } from "../src/providers/provider";
import { ProviderRouter } from "../src/providers/router";
import { JobProcessingError, publicJobError } from "../src/queue/job-error";
import type { ProcessingJob } from "../src/queue/job-queue";
import type { RateLimiter } from "../src/rate-limit/rate-limiter";
import type { ChatRequest, ChatResponse } from "../src/types/chat";

const request: ChatRequest = {
  model: "llama3.2",
  messages: [
    { role: "system", content: "private system instructions" },
    { role: "user", content: "private user prompt" },
  ],
};
const answer: ChatResponse = {
  message: { role: "assistant", content: "private generated answer" },
};
const modelMap = { "llama3.2": "hosted-model" };
const headers = { authorization: "Bearer test-api-key" };
const principal: ApiKeyPrincipal = {
  id: "key-1", name: "test key", keyPrefix: "mm_test",
  rateLimit: { capacity: 10, refillPerSecond: 1 },
};

class RecordingProvider implements LLMProvider {
  public readonly calls: { request: ChatRequest; context: ProviderContext | undefined }[] = [];

  public constructor(
    private readonly handler: (request: ChatRequest) => Promise<ChatResponse>,
  ) {}

  public chat(data: ChatRequest, context?: ProviderContext): Promise<ChatResponse> {
    this.calls.push({ request: data, context });
    return this.handler(data);
  }
}

function target(
  name: string,
  handler: (request: ChatRequest) => Promise<ChatResponse> = async () => answer,
  now: () => number = () => 0,
  failureThreshold = 2,
) {
  return {
    name,
    provider: new RecordingProvider(handler),
    circuit: new CircuitBreaker({ failureThreshold, resetTimeoutMs: 2_000, now }),
  };
}

function appOptions(provider: LLMProvider): BuildAppOptions {
  return {
    provider,
    apiKeyAuthenticator: {
      async authenticate(header) {
        if (header !== headers.authorization) throw new AuthenticationError();
        return principal;
      },
    },
    rateLimiter: {
      async consume() {
        return { allowed: true, remaining: 9, retryAfterSeconds: 0 };
      },
    },
    concurrencyLimiter: new InMemoryConcurrencyLimiter({
      maxConcurrent: 1, maxQueueSize: 1, waitTimeoutMs: 1_000,
    }),
    jobs: {
      async enqueue() { throw new Error("Unexpected job submission"); },
      async getJob() { throw new Error("Unexpected job lookup"); },
    },
    logger: false,
  };
}

test("router uses the primary response without invoking fallback", async () => {
  const primary = target("Ollama");
  const fallback = { ...target("Hosted"), modelMap };
  const router = new ProviderRouter(primary, fallback);

  assert.equal(await router.chat(request), answer);
  assert.equal(primary.provider.calls[0]?.request, request);
  assert.equal(fallback.provider.calls.length, 0);
  assert.deepEqual(router.snapshot(), [
    { name: "Ollama", state: "closed", consecutiveFailures: 0, retryAfterMs: 0 },
    { name: "Hosted", state: "closed", consecutiveFailures: 0, retryAfterMs: 0 },
  ]);
});

test("router falls back once on transient errors and maps only the model", async () => {
  for (const error of [
    new ProviderConnectionError("Ollama"),
    new ProviderTimeoutError("Ollama", 100),
    new ProviderHttpError("Ollama", 429, "private upstream detail"),
    new ProviderHttpError("Ollama", 500, "private upstream detail"),
    new ProviderHttpError("Ollama", 503, "private upstream detail"),
  ]) {
    const primary = target("Ollama", async () => { throw error; });
    const fallback = { ...target("Hosted"), modelMap };
    const router = new ProviderRouter(primary, fallback);

    assert.equal(await router.chat(request), answer);
    assert.equal(primary.provider.calls.length, 1);
    assert.equal(fallback.provider.calls.length, 1);
    assert.deepEqual(fallback.provider.calls[0]?.request, {
      ...request, model: "hosted-model",
    });
    assert.equal(request.model, "llama3.2");
    assert.equal(primary.circuit.snapshot().consecutiveFailures, 1);
    assert.equal(fallback.circuit.snapshot().consecutiveFailures, 0);
  }
});

test("permanent, malformed, and unexpected failures neither fall back nor trip circuits", async () => {
  for (const error of [
    new ProviderHttpError("Ollama", 400, "invalid request"),
    new ProviderHttpError("Ollama", 401, "invalid credentials"),
    new ProviderHttpError("Ollama", 404, "missing model"),
    new ProviderResponseError("Ollama"),
    new Error("unexpected application failure"),
  ]) {
    const primary = target("Ollama", async () => { throw error; }, () => 0, 1);
    const fallback = { ...target("Hosted"), modelMap };
    const router = new ProviderRouter(primary, fallback);

    await assert.rejects(router.chat(request), (actual) => actual === error);
    assert.equal(fallback.provider.calls.length, 0);
    assert.equal(primary.circuit.snapshot().state, "closed");
    assert.equal(primary.circuit.snapshot().consecutiveFailures, 0);
  }
});

test("fallback requires an own model mapping, including for object prototype names", async () => {
  for (const model of ["unmapped-model", "constructor", "toString", "__proto__"]) {
    const error = new ProviderConnectionError("Ollama");
    const primary = target("Ollama", async () => { throw error; });
    const fallback = { ...target("Hosted"), modelMap };
    const router = new ProviderRouter(primary, fallback);

    await assert.rejects(router.chat({ ...request, model }), (actual) => actual === error);
    assert.equal(fallback.provider.calls.length, 0);
  }
});

test("router preserves the primary error when no fallback is configured", async () => {
  const error = new ProviderTimeoutError("Ollama", 100);
  const primary = target("Ollama", async () => { throw error; });
  await assert.rejects(new ProviderRouter(primary).chat(request), (actual) => actual === error);
  assert.equal(primary.provider.calls.length, 1);
});

test("router reports the last attempted provider error without retrying either target", async () => {
  const error = new ProviderHttpError("Hosted", 401, "private credential detail");
  const primary = target("Ollama", async () => { throw new ProviderConnectionError("Ollama"); });
  const fallback = { ...target("Hosted", async () => { throw error; }), modelMap };

  await assert.rejects(new ProviderRouter(primary, fallback).chat(request), (actual) => actual === error);
  assert.equal(primary.provider.calls.length, 1);
  assert.equal(fallback.provider.calls.length, 1);
});

test("fallback successes leave a failing primary open and provider circuits stay independent", async () => {
  const primary = target("Ollama", async () => { throw new ProviderConnectionError("Ollama"); });
  let fallbackAttempts = 0;
  const fallback = {
    ...target("Hosted", async () => {
      if (++fallbackAttempts === 1) throw new ProviderTimeoutError("Hosted", 100);
      return answer;
    }),
    modelMap,
  };
  const router = new ProviderRouter(primary, fallback);

  await assert.rejects(router.chat(request), ProviderTimeoutError);
  assert.equal(await router.chat(request), answer);
  assert.equal(await router.chat(request), answer);
  assert.equal(primary.provider.calls.length, 2);
  assert.equal(fallback.provider.calls.length, 3);
  assert.deepEqual(router.snapshot(), [
    { name: "Ollama", state: "open", consecutiveFailures: 2, retryAfterMs: 2_000 },
    { name: "Hosted", state: "closed", consecutiveFailures: 0, retryAfterMs: 0 },
  ]);
});

test("both open circuits produce a typed 503 with Retry-After and no upstream calls", async () => {
  const primary = target("Ollama", async () => { throw new ProviderConnectionError("Ollama"); }, () => 0, 1);
  const fallback = {
    ...target("Hosted", async () => { throw new ProviderTimeoutError("Hosted", 100); }, () => 0, 1),
    modelMap,
  };
  const router = new ProviderRouter(primary, fallback);

  await assert.rejects(router.chat(request), ProviderTimeoutError);
  await assert.rejects(router.chat(request), (error: unknown) => {
    assert.ok(error instanceof ProviderCircuitOpenError);
    assert.equal(error.statusCode, 503);
    assert.equal(error.code, "PROVIDER_CIRCUIT_OPEN");
    assert.equal(error.retryAfterMs, 2_000);
    assert.equal(error.headers["retry-after"], "2");
    return true;
  });
  assert.equal(primary.provider.calls.length, 1);
  assert.equal(fallback.provider.calls.length, 1);
});

test("router recovers primary with one half-open probe while concurrent callers use fallback", async () => {
  let now = 0;
  let attempt = 0;
  let finishProbe!: (response: ChatResponse) => void;
  let reportProbeStarted!: () => void;
  const pendingProbe = new Promise<ChatResponse>((resolve) => { finishProbe = resolve; });
  const probeStarted = new Promise<void>((resolve) => { reportProbeStarted = resolve; });
  const primary = target("Ollama", async () => {
    attempt += 1;
    if (attempt === 1) throw new ProviderConnectionError("Ollama");
    if (attempt === 2) {
      reportProbeStarted();
      return pendingProbe;
    }
    return answer;
  }, () => now, 1);
  const fallback = { ...target("Hosted", async () => answer, () => now, 1), modelMap };
  const router = new ProviderRouter(primary, fallback);

  await router.chat(request);
  now = 1_999;
  await router.chat(request);
  assert.equal(primary.provider.calls.length, 1);
  now = 2_000;
  const probe = router.chat(request);
  await probeStarted;
  assert.equal(primary.circuit.snapshot().state, "half-open");
  await router.chat(request);
  assert.equal(primary.provider.calls.length, 2);
  assert.equal(fallback.provider.calls.length, 3);
  finishProbe(answer);
  assert.equal(await probe, answer);
  assert.equal(primary.circuit.snapshot().state, "closed");
  await router.chat(request);
  assert.equal(primary.provider.calls.length, 3);
  assert.equal(fallback.provider.calls.length, 3);
});

test("provider health requires authentication and rate limiting without making inference calls", async (t) => {
  const primary = target("Ollama");
  const fallback = { ...target("Hosted"), modelMap };
  const router = new ProviderRouter(primary, fallback);
  let allowed = true;
  let consumes = 0;
  let snapshots = 0;
  const rateLimiter: RateLimiter = {
    async consume(owner, policy) {
      assert.equal(owner, principal.id);
      assert.deepEqual(policy, principal.rateLimit);
      consumes += 1;
      return { allowed, remaining: allowed ? 9 : 0, retryAfterSeconds: allowed ? 0 : 3 };
    },
  };
  const app = buildApp({
    ...appOptions(router), rateLimiter,
    providerHealth: () => { snapshots += 1; return router.snapshot(); },
  });
  t.after(() => app.close());

  const unauthorized = await app.inject({ method: "GET", url: "/v1/providers" });
  assert.equal(unauthorized.statusCode, 401);
  assert.equal(consumes, 0);
  assert.equal(snapshots, 0);

  const health = await app.inject({ method: "GET", url: "/v1/providers", headers });
  assert.equal(health.statusCode, 200);
  assert.equal(health.headers["cache-control"], "no-store");
  assert.equal(health.headers["x-ratelimit-remaining"], "9");
  assert.deepEqual(health.json(), { scope: "process", providers: router.snapshot() });
  assert.equal(snapshots, 1);

  allowed = false;
  const limited = await app.inject({ method: "GET", url: "/v1/providers", headers });
  assert.equal(limited.statusCode, 429);
  assert.equal(limited.headers["retry-after"], "3");
  assert.equal(consumes, 2);
  assert.equal(snapshots, 1);

  const liveness = await app.inject({ method: "GET", url: "/health" });
  assert.equal(liveness.statusCode, 200);
  assert.deepEqual(liveness.json(), { status: "ok" });
  assert.equal(consumes, 2);
  assert.equal(primary.provider.calls.length, 0);
  assert.equal(fallback.provider.calls.length, 0);
});

test("provider health is absent when the app has no health snapshot callback", async (t) => {
  const app = buildApp(appOptions(target("Ollama").provider));
  t.after(() => app.close());
  const response = await app.inject({ method: "GET", url: "/v1/providers", headers });
  assert.equal(response.statusCode, 404);
});

test("chat releases capacity after fallback failure and open circuits, then admits recovery", async (t) => {
  let now = 0;
  let recovered = false;
  const primary = target("Ollama", async () => {
    if (!recovered) throw new ProviderConnectionError("Ollama");
    return answer;
  }, () => now, 1);
  const fallback = {
    ...target("Hosted", async () => { throw new ProviderTimeoutError("Hosted", 100); }, () => now, 1),
    modelMap,
  };
  const router = new ProviderRouter(primary, fallback);
  const options = appOptions(router);
  const app = buildApp(options);
  t.after(() => app.close());
  const submit = () => app.inject({
    method: "POST", url: "/v1/chat", payload: request,
    headers: { ...headers, "x-request-id": "chat-routing-request" },
  });

  const failed = await submit();
  assert.equal(failed.statusCode, 504);
  assert.equal(failed.json().error.code, "PROVIDER_TIMEOUT");
  assert.equal(options.concurrencyLimiter.snapshot().active, 0);

  const open = await submit();
  assert.equal(open.statusCode, 503);
  assert.equal(open.headers["retry-after"], "2");
  assert.equal(open.json().error.code, "PROVIDER_CIRCUIT_OPEN");
  assert.equal(open.json().error.requestId, "chat-routing-request");
  assert.equal(options.concurrencyLimiter.snapshot().active, 0);
  assert.equal(primary.provider.calls.length, 1);
  assert.equal(fallback.provider.calls.length, 1);

  now = 2_000;
  recovered = true;
  const success = await submit();
  assert.equal(success.statusCode, 200);
  assert.deepEqual(success.json(), answer);
  assert.equal(options.concurrencyLimiter.snapshot().active, 0);
  assert.equal(options.concurrencyLimiter.snapshot().admitted, 3);
});

test("chat router logs retain the HTTP request ID without logging messages or answers", async (t) => {
  const lines: string[] = [];
  const primary = target("Ollama", async () => { throw new ProviderConnectionError("Ollama"); });
  const fallback = { ...target("Hosted"), modelMap };
  const router = new ProviderRouter(primary, fallback);
  const app = buildApp({
    ...appOptions(router),
    logger: { level: "info", stream: { write(line: string) { lines.push(line); } } },
  });
  t.after(() => app.close());

  const response = await app.inject({
    method: "POST", url: "/v1/chat", payload: request,
    headers: { ...headers, "x-request-id": "routing-log-request" },
  });
  assert.equal(response.statusCode, 200);
  const routingEntries = lines.map((line) => JSON.parse(line)).filter((entry) =>
    entry.provider || entry.fallback,
  );
  assert.equal(routingEntries.length, 3);
  for (const entry of routingEntries) {
    assert.equal(entry.reqId, "routing-log-request");
    assert.equal(entry.apiKeyId, principal.id);
  }
  assert.equal(primary.provider.calls[0]?.context, fallback.provider.calls[0]?.context);
  assert.doesNotMatch(lines.join(""), /private system instructions|private user prompt|private generated answer/);
});

test("worker fallback keeps job correlation and prompt content out of routing logs", async () => {
  const lines: string[] = [];
  const log = pino({}, { write(line) { lines.push(line); } });
  const primary = target("Ollama", async () => { throw new ProviderConnectionError("Ollama"); });
  const fallback = { ...target("Hosted"), modelMap };
  const router = new ProviderRouter(primary, fallback);
  const job: ProcessingJob<ChatRequest> = {
    id: "job-routing", ownerId: principal.id, requestId: "job-request", attempt: 2, data: request,
  };

  assert.equal(await createInferenceProcessor(router, log)(job), answer);
  assert.equal(primary.provider.calls.length, 1);
  assert.equal(fallback.provider.calls.length, 1);
  assert.equal(primary.provider.calls[0]?.context, fallback.provider.calls[0]?.context);
  const entries = lines.map((line) => JSON.parse(line));
  assert.ok(entries.some((entry) => entry.provider === "Hosted"));
  for (const entry of entries) {
    assert.equal(entry.reqId, job.requestId);
    assert.equal(entry.jobId, job.id);
    assert.equal(entry.apiKeyId, job.ownerId);
    assert.equal(entry.attempt, job.attempt);
  }
  assert.doesNotMatch(lines.join(""), /private system instructions|private user prompt|private generated answer/);
});

test("worker classifies open circuits for durable retry and exposes a safe polling error", async () => {
  const primary = target("Ollama", async () => { throw new ProviderConnectionError("Ollama"); }, () => 0, 1);
  const fallback = {
    ...target("Hosted", async () => { throw new ProviderTimeoutError("Hosted", 100); }, () => 0, 1),
    modelMap,
  };
  const router = new ProviderRouter(primary, fallback);
  const processor = createInferenceProcessor(router, pino({ enabled: false }));
  const job: ProcessingJob<ChatRequest> = {
    id: "job-routing", ownerId: principal.id, requestId: "job-request", attempt: 1, data: request,
  };

  await assert.rejects(processor(job), (error: unknown) => {
    assert.ok(error instanceof JobProcessingError);
    assert.equal(error.code, "PROVIDER_TIMEOUT");
    assert.equal(error.retryable, true);
    assert.equal(error.retryAfterMs, 0);
    return true;
  });
  await assert.rejects(processor({ ...job, attempt: 2 }), (error: unknown) => {
    assert.ok(error instanceof JobProcessingError);
    assert.equal(error.code, "PROVIDER_CIRCUIT_OPEN");
    assert.equal(error.retryable, true);
    assert.equal(error.retryAfterMs, 2_000);
    assert.deepEqual(publicJobError(error.code), {
      code: "PROVIDER_CIRCUIT_OPEN", message: "Inference provider is temporarily unavailable",
    });
    return true;
  });
  assert.equal(primary.provider.calls.length, 1);
  assert.equal(fallback.provider.calls.length, 1);
});
