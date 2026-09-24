import type { FastifyPluginAsync, FastifySchema } from "fastify";
import type { ConcurrencyLimiter } from "../concurrency/concurrency-limiter";
import type { LLMProvider } from "../providers/provider";
import type { ChatRequest, ChatResponse } from "../types/chat";
import { chatRequestSchema, chatResponseSchema } from "./chat-schema";

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
