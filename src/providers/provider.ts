import type { ChatRequest, ChatResponse } from "../types/chat.js";

export interface LLMProvider {
  chat(request: ChatRequest): Promise<ChatResponse>;
}
