import assert from "node:assert/strict";
import test from "node:test";
import { buildApp } from "../src/app";
import { InMemoryConcurrencyLimiter } from "../src/concurrency/in-memory-concurrency-limiter";
import { AuthenticationError, IdempotencyConflictError, JobNotFoundError, JobQueueUnavailableError } from "../src/errors";
import type { InferenceJobs } from "../src/jobs/inference-jobs";
import type { RateLimiter } from "../src/rate-limit/rate-limiter";
import type { ChatRequest } from "../src/types/chat";

const headers = { authorization: "Bearer owner-1", "idempotency-key": "submission-1" };
const payload: ChatRequest = { model: "llama3.2", messages: [{ role: "user", content: "Hello" }] };
const receipt = { id: "job-123", requestId: "original-request" };

function makeApp(overrides: Partial<InferenceJobs> = {}, rateLimiter: RateLimiter = {
  async consume() { return { allowed: true, remaining: 9, retryAfterSeconds: 0 }; },
}) {
  return buildApp({
    logger: false,
    provider: { async chat() { throw new Error("Job routes must not call the provider"); } },
    apiKeyAuthenticator: {
      async authenticate(header) {
        if (header !== headers.authorization) throw new AuthenticationError();
        return { id: "owner-1", name: "test", keyPrefix: "mm_test", rateLimit: { capacity: 10, refillPerSecond: 1 } };
      },
    },
    rateLimiter,
    concurrencyLimiter: new InMemoryConcurrencyLimiter({ maxConcurrent: 1, maxQueueSize: 0, waitTimeoutMs: 100 }),
    jobs: {
      async enqueue() { throw new Error("Unexpected enqueue"); },
      async getJob() { throw new Error("Unexpected lookup"); },
      ...overrides,
    },
  });
}

test("POST /v1/jobs passes ownership, idempotency key, and request ID to the generic queue", async (t) => {
  const app = makeApp({
    async enqueue(submission) {
      assert.deepEqual(submission, {
        ownerId: "owner-1",
        requestId: "http-request-1",
        idempotencyKey: "submission-1",
        data: payload,
      });
      return receipt;
    },
  });
  t.after(() => app.close());

  const response = await app.inject({
    method: "POST", url: "/v1/jobs", payload,
    headers: { ...headers, "x-request-id": "http-request-1" },
  });
  assert.equal(response.statusCode, 202);
  assert.equal(response.headers.location, "/v1/jobs/job-123");
  assert.equal(response.headers["x-request-id"], "http-request-1");
  assert.deepEqual(response.json(), receipt);
});

test("job submission validates chat input and requires a bounded idempotency key", async (t) => {
  const app = makeApp();
  t.after(() => app.close());
  for (const invalid of [
    { headers: { authorization: headers.authorization }, payload },
    { headers: { ...headers, "idempotency-key": " " }, payload },
    { headers: { ...headers, "idempotency-key": "a".repeat(129) }, payload },
    { headers, payload: { model: "llama3.2" } },
    { headers, payload: { ...payload, messages: [] } },
    { headers, payload: { ...payload, messages: [{ role: "invalid", content: "hello" }] } },
  ]) {
    const response = await app.inject({ method: "POST", url: "/v1/jobs", ...invalid });
    assert.equal(response.statusCode, 400);
    assert.equal(response.json().error.code, "VALIDATION_ERROR");
  }
});

test("submission and polling require authentication and consume the per-key rate limit", async (t) => {
  const app = makeApp({}, {
    async consume(owner) {
      assert.equal(owner, "owner-1");
      return { allowed: false, remaining: 0, retryAfterSeconds: 2 };
    },
  });
  t.after(() => app.close());
  for (const route of [{ method: "POST" as const, url: "/v1/jobs", payload }, { method: "GET" as const, url: "/v1/jobs/job-123" }]) {
    const unauthorized = await app.inject(route);
    assert.equal(unauthorized.statusCode, 401);
    const limited = await app.inject({ ...route, headers });
    assert.equal(limited.statusCode, 429);
    assert.equal(limited.headers["retry-after"], "2");
  }
});

test("job status returns a provider-neutral response and excludes internal fields", async (t) => {
  const app = makeApp({
    async getJob(id, ownerId) {
      assert.equal(id, receipt.id);
      assert.equal(ownerId, "owner-1");
      return {
        ...receipt, status: "completed", attemptsMade: 1,
        response: { message: { role: "assistant", content: "Hello back" } },
        internalPrompt: "must not appear",
      };
    },
  });
  t.after(() => app.close());
  const response = await app.inject({ method: "GET", url: "/v1/jobs/job-123", headers });
  assert.equal(response.statusCode, 200);
  assert.equal(response.headers["cache-control"], "no-store");
  assert.deepEqual(response.json(), {
    ...receipt, status: "completed", attemptsMade: 1,
    response: { message: { role: "assistant", content: "Hello back" } },
  });
});

test("queue errors use centralized HTTP error handling", async (t) => {
  for (const error of [new JobNotFoundError(), new IdempotencyConflictError(), new JobQueueUnavailableError(new Error("private connection detail"))]) {
    const app = makeApp({ async enqueue() { throw error; }, async getJob() { throw error; } });
    t.after(() => app.close());
    const response = await app.inject(error instanceof JobNotFoundError
      ? { method: "GET", url: "/v1/jobs/job-123", headers }
      : { method: "POST", url: "/v1/jobs", headers, payload });
    assert.equal(response.statusCode, error.statusCode);
    assert.equal(response.json().error.code, error.code);
    assert.ok(response.json().error.requestId);
    assert.doesNotMatch(response.body, /private connection detail/);
  }
});

test("failed jobs are retrieved successfully with a terminal error, rather than an HTTP failure", async (t) => {
  const status = { ...receipt, status: "failed" as const, attemptsMade: 3, error: { code: "PROVIDER_TIMEOUT", message: "Inference provider request timed out" } };
  const app = makeApp({ async getJob() { return status; } });
  t.after(() => app.close());
  const response = await app.inject({ method: "GET", url: "/v1/jobs/job-123", headers });
  assert.equal(response.statusCode, 200);
  assert.deepEqual(response.json(), status);
});
