import type { ChatRequest, ChatResponse } from "../types/chat";

export interface LLMProvider {
  chat(request: ChatRequest): Promise<ChatResponse>;
}
