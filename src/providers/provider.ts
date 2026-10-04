import type { Logger } from "pino";
import type { ChatRequest, ChatResponse } from "../types/chat";

export interface ProviderContext {
  logger?: Pick<Logger, "info" | "warn">;
  signal?: AbortSignal;
}

// Awaiting the consumer is required: it carries backpressure upstream.
export type ChatDeltaConsumer = (content: string) => Promise<void>;

export interface LLMProvider {
  chat(request: ChatRequest, context?: ProviderContext): Promise<ChatResponse>;
  stream?(request: ChatRequest, onDelta: ChatDeltaConsumer, context?: ProviderContext): Promise<void>;
}
