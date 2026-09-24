import { randomUUID } from "node:crypto";
import fastify, {
  type FastifyError,
  type FastifyInstance,
  type FastifyServerOptions,
} from "fastify";
import type { ApiKeyAuthenticator } from "./auth/api-keys";
import { configureApiKeyGuard } from "./auth/guard";
import type { ConcurrencyLimiter } from "./concurrency/concurrency-limiter";
import { AppError } from "./errors";
import type { InferenceJobs } from "./jobs/inference-jobs";
import type { LLMProvider } from "./providers/provider";
import type { RateLimiter } from "./rate-limit/rate-limiter";
import { chatRoutes } from "./routes/chat";
import { jobRoutes } from "./routes/jobs";

export interface BuildAppOptions {
  provider: LLMProvider;
  apiKeyAuthenticator: ApiKeyAuthenticator;
  rateLimiter: RateLimiter;
  concurrencyLimiter: ConcurrencyLimiter;
  jobs: InferenceJobs;
  logger?: FastifyServerOptions["logger"];
}

export function buildApp({
  provider,
  apiKeyAuthenticator,
  rateLimiter,
  concurrencyLimiter,
  jobs,
  logger = { level: "info" },
}: BuildAppOptions): FastifyInstance {
  const app = fastify({
    logger,
    requestIdHeader: "x-request-id",
    genReqId: () => randomUUID(),
  });

  app.addHook("onRequest", async (request, reply) => {
    void reply.header("x-request-id", request.id);
  });

  app.get("/health", async () => ({ status: "ok" }));
  void app.register(async (protectedApp) => {
    configureApiKeyGuard(protectedApp, {
      authenticator: apiKeyAuthenticator,
      rateLimiter,
    });
    await protectedApp.register(chatRoutes, { provider, concurrencyLimiter });
    await protectedApp.register(jobRoutes, { jobs });
  });

  app.setErrorHandler<FastifyError>((error, request, reply) => {
    if (error.validation) {
      void reply.status(400).send({
        error: {
          code: "VALIDATION_ERROR",
          message: `Invalid request ${error.validationContext ?? "body"}`,
          details: error.validation.map(({ instancePath, message }) => ({
            path: instancePath || "/",
            message: message ?? "is invalid",
          })),
          requestId: request.id,
        },
      });
      return;
    }

    if (error instanceof AppError) {
      const logContext = { err: error, code: error.code };

      if (error.statusCode >= 500) {
        request.log.error(logContext, "Request failed");
      } else {
        request.log.warn(logContext, "Request rejected");
      }

      void reply.headers(error.headers);
      void reply.status(error.statusCode).send({
        error: {
          code: error.code,
          message: error.message,
          requestId: request.id,
        },
      });
      return;
    }

    if (
      typeof error.statusCode === "number" &&
      error.statusCode >= 400 &&
      error.statusCode < 500
    ) {
      request.log.warn({ err: error }, "Client request rejected");
      void reply.status(error.statusCode).send({
        error: {
          code: error.code || "BAD_REQUEST",
          message: error.message,
          requestId: request.id,
        },
      });
      return;
    }

    request.log.error({ err: error }, "Unhandled request error");
    void reply.status(500).send({
      error: {
        code: "INTERNAL_SERVER_ERROR",
        message: "Internal server error",
        requestId: request.id,
      },
    });
  });

  return app;
}
