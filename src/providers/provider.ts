import type { Logger } from "pino";
import type { ChatRequest, ChatResponse } from "../types/chat";

export interface ProviderContext {
  logger: Pick<Logger, "info" | "warn">;
}

export interface LLMProvider {
  chat(request: ChatRequest, context?: ProviderContext): Promise<ChatResponse>;
}
