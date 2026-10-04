import { setMaxListeners } from "node:events";
import type { FastifyPluginAsync, FastifySchema } from "fastify";
import { RequestAbortedError, throwIfCancelled } from "../cancellation";
import { AppError, ProviderStreamingUnsupportedError } from "../errors";
import type { ConcurrencyLimiter, ConcurrencyPermit } from "../concurrency/concurrency-limiter";
import type { LLMProvider } from "../providers/provider";
import type { ChatRequest, ChatResponse } from "../types/chat";
import { chatRequestSchema, chatResponseSchema } from "./chat-schema";
import { requestCancellation } from "./request-cancellation";
import { writeSse } from "./sse";

export interface ChatRoutesOptions {
  provider: LLMProvider;
  concurrencyLimiter: ConcurrencyLimiter;
}

const chatSchema = {
  body: chatRequestSchema,
  response: {
    200: chatResponseSchema,
  },
} satisfies FastifySchema;

export const chatRoutes: FastifyPluginAsync<ChatRoutesOptions> = async (
  app,
  { provider, concurrencyLimiter },
) => {
  const shutdown = new AbortController();
  // Every active/queued request subscribes; each removes its listener in finally.
  setMaxListeners(0, shutdown.signal);
  app.addHook("preClose", async () => { shutdown.abort(); });

  app.post<{ Body: ChatRequest; Reply: ChatResponse }>(
    "/v1/chat",
    { schema: chatSchema },
    async (request, response) => {
      const cancellation = requestCancellation(request, response, shutdown.signal);
      let permit: ConcurrencyPermit | undefined;
      try {
        permit = await concurrencyLimiter.acquire(cancellation.signal);
        throwIfCancelled(cancellation.signal);

        request.log.info(
          {
            concurrencyWaitTimeMs: permit.waitTimeMs,
            concurrencyActive: permit.activeAtAdmission,
            concurrencyQueued: permit.queuedAtAdmission,
          },
          "Provider execution admitted",
        );

        return await provider.chat(request.body, { logger: request.log, signal: cancellation.signal });
      } catch (error) {
        if (error instanceof RequestAbortedError || cancellation.signal.aborted) {
          request.log.info({ abortedPhase: permit ? "execution" : "queue" }, "Chat request cancelled");
          response.hijack();
          response.raw.destroy();
          return;
        }
        throw error;
      } finally {
        cancellation.abort();
        cancellation.dispose();
        permit?.release();
      }
    },
  );

  app.post<{ Body: ChatRequest }>("/v1/chat/stream", {
    schema: { body: chatRequestSchema },
  }, async (request, response) => {
    const cancellation = requestCancellation(request, response, shutdown.signal);
    let permit: ConcurrencyPermit | undefined;
    let started = false;
    const start = () => {
      throwIfCancelled(cancellation.signal);
      if (started) return;
      response.hijack();
      response.headers({
        "content-type": "text/event-stream; charset=utf-8",
        "cache-control": "no-cache, no-transform",
        "x-accel-buffering": "no",
      });
      for (const [name, value] of Object.entries(response.getHeaders())) {
        if (value !== undefined) response.raw.setHeader(name, value);
      }
      response.raw.writeHead(200);
      started = true;
    };
    try {
      permit = await concurrencyLimiter.acquire(cancellation.signal);
      throwIfCancelled(cancellation.signal);
      if (!provider.stream) throw new ProviderStreamingUnsupportedError();
      request.log.info({
        concurrencyWaitTimeMs: permit.waitTimeMs,
        concurrencyActive: permit.activeAtAdmission,
        concurrencyQueued: permit.queuedAtAdmission,
      }, "Streaming execution admitted");
      await provider.stream(request.body, async (content) => {
        if (!content) return;
        start();
        try {
          await writeSse(response.raw, "delta", { content, requestId: request.id }, cancellation.signal);
        } catch (error) {
          cancellation.abort();
          throw error;
        }
      }, { logger: request.log, signal: cancellation.signal });
      start(); // A valid empty answer still receives a done event.
      await writeSse(response.raw, "done", { requestId: request.id }, cancellation.signal);
      response.raw.end();
    } catch (error) {
      if (error instanceof RequestAbortedError || cancellation.signal.aborted) {
        request.log.info({ abortedPhase: permit ? "execution" : "queue" }, "Stream cancelled or client stopped reading");
        response.hijack();
        response.raw.destroy();
      } else if (!started) {
        throw error; // Normal JSON/HTTP error handling before SSE starts.
      } else {
        request.log.warn({ code: error instanceof AppError ? error.code : "INTERNAL_SERVER_ERROR" }, "Stream failed after output");
        // A timeout may interrupt a pending downstream write. Do not enqueue a
        // second event behind it; close the transport and let the client detect EOF.
        if (response.raw.writableNeedDrain) {
          response.raw.destroy();
          return;
        }
        try {
          await writeSse(response.raw, "error", {
            code: error instanceof AppError ? error.code : "INTERNAL_SERVER_ERROR",
            message: "The response could not be completed",
            requestId: request.id,
          }, cancellation.signal);
          response.raw.end();
        } catch { response.raw.destroy(); }
      }
    } finally {
      cancellation.abort();
      cancellation.dispose();
      permit?.release();
    }
  });
};
