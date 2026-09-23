import type { FastifyPluginAsync, FastifySchema } from "fastify";
import type { ConcurrencyLimiter } from "../concurrency/concurrency-limiter";
import type { LLMProvider } from "../providers/provider";
import type { ChatRequest, ChatResponse } from "../types/chat";

export interface ChatRoutesOptions {
  provider: LLMProvider;
  concurrencyLimiter: ConcurrencyLimiter;
}

const chatSchema = {
  body: {
    type: "object",
    additionalProperties: false,
    required: ["model", "messages"],
    properties: {
      model: { type: "string", minLength: 1 },
      messages: {
        type: "array",
        minItems: 1,
        items: {
          type: "object",
          additionalProperties: false,
          required: ["role", "content"],
          properties: {
            role: { type: "string", enum: ["system", "user", "assistant"] },
            content: { type: "string", minLength: 1 },
          },
        },
      },
    },
  },
  response: {
    200: {
      type: "object",
      additionalProperties: false,
      required: ["message"],
      properties: {
        message: {
          type: "object",
          additionalProperties: false,
          required: ["role", "content"],
          properties: {
            role: { type: "string", enum: ["assistant"] },
            content: { type: "string" },
          },
        },
      },
    },
  },
} satisfies FastifySchema;

export const chatRoutes: FastifyPluginAsync<ChatRoutesOptions> = async (
  app,
  { provider, concurrencyLimiter },
) => {
  app.post<{ Body: ChatRequest; Reply: ChatResponse }>(
    "/v1/chat",
    { schema: chatSchema },
    async (request) => {
      const permit = await concurrencyLimiter.acquire();

      request.log.info(
        {
          concurrencyWaitTimeMs: permit.waitTimeMs,
          concurrencyActive: permit.activeAtAdmission,
          concurrencyQueued: permit.queuedAtAdmission,
        },
        "Provider execution admitted",
      );

      try {
        return await provider.chat(request.body);
      } finally {
        permit.release();
      }
    },
  );
};
