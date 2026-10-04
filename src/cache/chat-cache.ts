import type { ChatRequest, ChatResponse } from "../types/chat";

// Only completed, non-streaming answers belong here. Errors are handled by the
// route as cache bypasses; request cancellation must still propagate.
export interface ChatCache {
  get(ownerId: string, request: ChatRequest, signal: AbortSignal): Promise<ChatResponse | null>;
  put(ownerId: string, request: ChatRequest, response: ChatResponse, signal: AbortSignal): Promise<void>;
}
