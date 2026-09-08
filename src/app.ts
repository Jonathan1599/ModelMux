import { randomUUID } from "node:crypto";
import fastify, {
  type FastifyError,
  type FastifyInstance,
  type FastifyServerOptions,
} from "fastify";
import { AppError } from "./errors.js";
import type { LLMProvider } from "./providers/provider.js";
import { chatRoutes } from "./routes/chat.js";

export interface BuildAppOptions {
  provider: LLMProvider;
  logger?: FastifyServerOptions["logger"];
}

export function buildApp({
  provider,
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
  void app.register(chatRoutes, { provider });

  app.setErrorHandler<FastifyError>((error, request, reply) => {
    if (error.validation) {
      void reply.status(400).send({
        error: {
          code: "VALIDATION_ERROR",
          message: "Invalid request body",
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
      request.log.error({ err: error, code: error.code }, "Provider request failed");
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
