import { buildApp } from "../../src/app";
import { AuthenticationError } from "../../src/errors";
import { InMemoryConcurrencyLimiter } from "../../src/concurrency/in-memory-concurrency-limiter";
import type { LLMProvider } from "../../src/providers/provider";

export const chatPayload = { model: "local", messages: [{ role: "user", content: "Hello" }] };
export const chatHeaders = { authorization: "Bearer test-key", "x-request-id": "stream-request" };

export function chatApp(provider: LLMProvider, maxQueueSize = 2) {
  const limiter = new InMemoryConcurrencyLimiter({ maxConcurrent: 1, maxQueueSize, waitTimeoutMs: 1000 });
  let allowed = true;
  const app = buildApp({
    provider, logger: false, concurrencyLimiter: limiter,
    apiKeyAuthenticator: { async authenticate(header) {
      if (header !== chatHeaders.authorization) throw new AuthenticationError();
      return { id: "owner", name: "test", keyPrefix: "test", rateLimit: { capacity: 10, refillPerSecond: 1 } };
    } },
    rateLimiter: { async consume() { return { allowed, remaining: 9, retryAfterSeconds: allowed ? 0 : 1 }; } },
    jobs: {
      async enqueue() { throw new Error("Streaming must not use the broker"); },
      async getJob() { throw new Error("Unexpected polling"); },
    },
  });
  return { app, limiter, limitRequests() { allowed = false; } };
}
