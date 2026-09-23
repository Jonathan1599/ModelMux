import assert from "node:assert/strict";
import test from "node:test";
import { buildApp } from "../src/app";
import type {
  ApiKeyAuthenticator,
  ApiKeyPrincipal,
} from "../src/auth/api-keys";
import type {
  ConcurrencyLimiter,
  ConcurrencyPermit,
  ConcurrencySnapshot,
} from "../src/concurrency/concurrency-limiter";
import { InMemoryConcurrencyLimiter } from "../src/concurrency/in-memory-concurrency-limiter";
import {
  AuthenticationError,
  ConcurrencyQueueFullError,
  ProviderConnectionError,
  ProviderHttpError,
  ProviderTimeoutError,
} from "../src/errors";
import type { LLMProvider } from "../src/providers/provider";
import type {
  RateLimitDecision,
  RateLimiter,
  RateLimitPolicy,
} from "../src/rate-limit/rate-limiter";
import type { ChatRequest, ChatResponse } from "../src/types/chat";

const authorizationHeaders = { authorization: "Bearer test-api-key" };
const testPrincipal: ApiKeyPrincipal = {
  id: "key-1",
  name: "test key",
  keyPrefix: "mm_test",
  rateLimit: { capacity: 10, refillPerSecond: 2 },
};

class StubProvider implements LLMProvider {
  public constructor(
    private readonly handler: (request: ChatRequest) => Promise<ChatResponse>,
  ) {}

  public chat(request: ChatRequest): Promise<ChatResponse> {
    return this.handler(request);
  }
}

class TestAuthenticator implements ApiKeyAuthenticator {
  public async authenticate(
    authorizationHeader: string | undefined,
  ): Promise<ApiKeyPrincipal> {
    if (authorizationHeader !== authorizationHeaders.authorization) {
      throw new AuthenticationError();
    }

    return testPrincipal;
  }
}

class AllowingRateLimiter implements RateLimiter {
  public async consume(): Promise<RateLimitDecision> {
    return { allowed: true, remaining: 9, retryAfterSeconds: 0 };
  }
}

function buildTestApp(
  provider: LLMProvider,
  apiKeyAuthenticator: ApiKeyAuthenticator = new TestAuthenticator(),
  rateLimiter: RateLimiter = new AllowingRateLimiter(),
  concurrencyLimiter: ConcurrencyLimiter = new InMemoryConcurrencyLimiter({
    maxConcurrent: 10,
    maxQueueSize: 10,
    waitTimeoutMs: 1_000,
  }),
) {
  return buildApp({
    provider,
    apiKeyAuthenticator,
    rateLimiter,
    concurrencyLimiter,
    logger: false,
  });
}

test("GET /health reports that the process is up", async (t) => {
  const provider = new StubProvider(async () => {
    throw new Error("Provider should not be called");
  });
  const app = buildTestApp(provider);
  t.after(() => app.close());

  const response = await app.inject({ method: "GET", url: "/health" });

  assert.equal(response.statusCode, 200);
  assert.deepEqual(response.json(), { status: "ok" });
  assert.ok(response.headers["x-request-id"]);
});

test("POST /v1/chat forwards a valid provider-agnostic request", async (t) => {
  let receivedRequest: ChatRequest | undefined;
  const provider = new StubProvider(async (request) => {
    receivedRequest = request;
    return {
      message: {
        role: "assistant",
        content: "TCP reliably delivers an ordered stream of bytes between hosts.",
      },
    };
  });
  const app = buildTestApp(provider);
  t.after(() => app.close());

  const payload: ChatRequest = {
    model: "llama3.2",
    messages: [{ role: "user", content: "Explain TCP in one sentence." }],
  };
  const response = await app.inject({
    method: "POST",
    url: "/v1/chat",
    headers: authorizationHeaders,
    payload,
  });

  assert.equal(response.statusCode, 200);
  assert.equal(response.headers["x-ratelimit-limit"], "10");
  assert.equal(response.headers["x-ratelimit-remaining"], "9");
  assert.deepEqual(receivedRequest, payload);
  assert.deepEqual(response.json(), {
    message: {
      role: "assistant",
      content: "TCP reliably delivers an ordered stream of bytes between hosts.",
    },
  });
});

test("POST /v1/chat rejects malformed request bodies", async (t) => {
  let callCount = 0;
  const provider = new StubProvider(async () => {
    callCount += 1;
    return { message: { role: "assistant", content: "unexpected" } };
  });
  const app = buildTestApp(provider);
  t.after(() => app.close());

  const invalidPayloads = [
    { model: "llama3.2" },
    { model: "llama3.2", messages: [] },
    {
      model: "llama3.2",
      messages: [{ role: "invalid", content: "hello" }],
    },
  ];

  for (const payload of invalidPayloads) {
    const response = await app.inject({
      method: "POST",
      url: "/v1/chat",
      headers: authorizationHeaders,
      payload,
    });

    assert.equal(response.statusCode, 400);
    assert.equal(response.json().error.code, "VALIDATION_ERROR");
  }

  assert.equal(callCount, 0);
});

test("POST /v1/chat returns 400 for invalid JSON", async (t) => {
  const provider = new StubProvider(async () => {
    throw new Error("Provider should not be called");
  });
  const app = buildTestApp(provider);
  t.after(() => app.close());

  const response = await app.inject({
    method: "POST",
    url: "/v1/chat",
    headers: {
      ...authorizationHeaders,
      "content-type": "application/json",
    },
    payload: '{"model":',
  });

  assert.equal(response.statusCode, 400);
  assert.equal(response.json().error.code, "FST_ERR_CTP_INVALID_JSON_BODY");
});

test("POST /v1/chat maps provider connection failures to 503", async (t) => {
  const provider = new StubProvider(async () => {
    throw new ProviderConnectionError("Ollama");
  });
  const app = buildTestApp(provider);
  t.after(() => app.close());

  const response = await app.inject({
    method: "POST",
    url: "/v1/chat",
    headers: authorizationHeaders,
    payload: {
      model: "llama3.2",
      messages: [{ role: "user", content: "Hello" }],
    },
  });

  assert.equal(response.statusCode, 503);
  assert.equal(response.json().error.code, "PROVIDER_UNAVAILABLE");
  assert.equal(response.json().error.message, "Unable to connect to Ollama");
});

test("POST /v1/chat maps provider timeouts to 504", async (t) => {
  const provider = new StubProvider(async () => {
    throw new ProviderTimeoutError("Ollama", 120_000);
  });
  const app = buildTestApp(provider);
  t.after(() => app.close());

  const response = await app.inject({
    method: "POST",
    url: "/v1/chat",
    headers: authorizationHeaders,
    payload: {
      model: "llama3.2",
      messages: [{ role: "user", content: "Hello" }],
    },
  });

  assert.equal(response.statusCode, 504);
  assert.equal(response.json().error.code, "PROVIDER_TIMEOUT");
  assert.equal(response.json().error.message, "Ollama request timed out");
});

test("POST /v1/chat does not expose upstream provider details", async (t) => {
  const provider = new StubProvider(async () => {
    throw new ProviderHttpError("Ollama", 500, "sensitive upstream detail");
  });
  const app = buildTestApp(provider);
  t.after(() => app.close());

  const response = await app.inject({
    method: "POST",
    url: "/v1/chat",
    headers: authorizationHeaders,
    payload: {
      model: "llama3.2",
      messages: [{ role: "user", content: "Hello" }],
    },
  });

  assert.equal(response.statusCode, 502);
  assert.equal(response.json().error.code, "PROVIDER_ERROR");
  assert.equal(response.json().error.message, "Ollama request failed");
  assert.doesNotMatch(response.body, /sensitive upstream detail/);
});

test("POST /v1/chat requires a valid API key", async (t) => {
  let providerCalls = 0;
  const provider = new StubProvider(async () => {
    providerCalls += 1;
    return { message: { role: "assistant", content: "unexpected" } };
  });
  const app = buildTestApp(provider);
  t.after(() => app.close());

  const response = await app.inject({
    method: "POST",
    url: "/v1/chat",
    payload: {
      model: "llama3.2",
      messages: [{ role: "user", content: "Hello" }],
    },
  });

  assert.equal(response.statusCode, 401);
  assert.equal(response.json().error.code, "INVALID_API_KEY");
  assert.equal(response.headers["www-authenticate"], 'Bearer realm="modelmux"');
  assert.equal(providerCalls, 0);
});

test("POST /v1/chat enforces the API key's rate-limit policy", async (t) => {
  let providerCalls = 0;
  let consumedSubject: string | undefined;
  let consumedPolicy: RateLimitPolicy | undefined;
  const provider = new StubProvider(async () => {
    providerCalls += 1;
    return { message: { role: "assistant", content: "unexpected" } };
  });
  const rateLimiter: RateLimiter = {
    async consume(subject, policy) {
      consumedSubject = subject;
      consumedPolicy = policy;
      return { allowed: false, remaining: 0, retryAfterSeconds: 3 };
    },
  };
  const app = buildTestApp(provider, new TestAuthenticator(), rateLimiter);
  t.after(() => app.close());

  const response = await app.inject({
    method: "POST",
    url: "/v1/chat",
    headers: authorizationHeaders,
    payload: {
      model: "llama3.2",
      messages: [{ role: "user", content: "Hello" }],
    },
  });

  assert.equal(response.statusCode, 429);
  assert.equal(response.json().error.code, "RATE_LIMIT_EXCEEDED");
  assert.equal(response.headers["retry-after"], "3");
  assert.equal(response.headers["x-ratelimit-limit"], "10");
  assert.equal(response.headers["x-ratelimit-remaining"], "0");
  assert.equal(consumedSubject, testPrincipal.id);
  assert.deepEqual(consumedPolicy, testPrincipal.rateLimit);
  assert.equal(providerCalls, 0);
});

test("POST /v1/chat returns 503 when the concurrency queue is full", async (t) => {
  let providerCalls = 0;
  const provider = new StubProvider(async () => {
    providerCalls += 1;
    return { message: { role: "assistant", content: "unexpected" } };
  });
  const concurrencyLimiter: ConcurrencyLimiter = {
    async acquire() {
      throw new ConcurrencyQueueFullError(2, 20);
    },
    snapshot() {
      return emptyConcurrencySnapshot();
    },
  };
  const app = buildTestApp(
    provider,
    new TestAuthenticator(),
    new AllowingRateLimiter(),
    concurrencyLimiter,
  );
  t.after(() => app.close());

  const response = await app.inject({
    method: "POST",
    url: "/v1/chat",
    headers: authorizationHeaders,
    payload: {
      model: "llama3.2",
      messages: [{ role: "user", content: "Hello" }],
    },
  });

  assert.equal(response.statusCode, 503);
  assert.equal(response.json().error.code, "CONCURRENCY_QUEUE_FULL");
  assert.equal(response.headers["retry-after"], "1");
  assert.equal(providerCalls, 0);
});

test("POST /v1/chat releases its concurrency permit after provider failure", async (t) => {
  let releaseCalls = 0;
  const provider = new StubProvider(async () => {
    throw new ProviderConnectionError("Ollama");
  });
  const concurrencyLimiter: ConcurrencyLimiter = {
    async acquire(): Promise<ConcurrencyPermit> {
      return {
        waitTimeMs: 0,
        activeAtAdmission: 1,
        queuedAtAdmission: 0,
        release() {
          releaseCalls += 1;
        },
      };
    },
    snapshot() {
      return emptyConcurrencySnapshot();
    },
  };
  const app = buildTestApp(
    provider,
    new TestAuthenticator(),
    new AllowingRateLimiter(),
    concurrencyLimiter,
  );
  t.after(() => app.close());

  const response = await app.inject({
    method: "POST",
    url: "/v1/chat",
    headers: authorizationHeaders,
    payload: {
      model: "llama3.2",
      messages: [{ role: "user", content: "Hello" }],
    },
  });

  assert.equal(response.statusCode, 503);
  assert.equal(releaseCalls, 1);
});

function emptyConcurrencySnapshot(): ConcurrencySnapshot {
  return {
    active: 0,
    queued: 0,
    admitted: 0,
    rejected: 0,
    timedOut: 0,
    totalWaitTimeMs: 0,
    maxWaitTimeMs: 0,
  };
}
