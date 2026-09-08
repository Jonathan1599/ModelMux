export type ChatRole = "system" | "user" | "assistant";

export interface ChatMessage {
  role: ChatRole;
  content: string;
}

export interface ChatRequest {
  model: string;
  messages: ChatMessage[];
}

export interface AssistantMessage extends ChatMessage {
  role: "assistant";
}

export interface ChatResponse {
  message: AssistantMessage;
}
