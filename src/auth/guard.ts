import type { FastifyInstance } from "fastify";
import { RateLimitExceededError } from "../errors";
import type { RateLimiter } from "../rate-limit/rate-limiter";
import type { ApiKeyAuthenticator, ApiKeyPrincipal } from "./api-keys";
import { recordMetric, type MetricSink } from "../observability/telemetry";

declare module "fastify" {
  interface FastifyRequest {
    apiKey: ApiKeyPrincipal | null;
  }
}

export interface ApiKeyGuardOptions {
  authenticator: ApiKeyAuthenticator;
  rateLimiter: RateLimiter;
  metrics?: MetricSink | undefined;
}

export function configureApiKeyGuard(
  app: FastifyInstance,
  { authenticator, rateLimiter, metrics }: ApiKeyGuardOptions,
): void {
  app.decorateRequest("apiKey", null);

  app.addHook("onRequest", async (request, reply) => {
    const apiKey = await authenticator.authenticate(request.headers.authorization);
    request.apiKey = apiKey;
    request.log = request.log.child({ apiKeyId: apiKey.id });

    const decision = await rateLimiter.consume(apiKey.id, apiKey.rateLimit);

    void reply.headers({
      "x-ratelimit-limit": apiKey.rateLimit.capacity,
      "x-ratelimit-remaining": decision.remaining,
    });

    if (!decision.allowed) {
      recordMetric(metrics, { kind: "rate_limit_rejection" });
      throw new RateLimitExceededError(decision.retryAfterSeconds);
    }
  });
}
