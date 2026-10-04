import { createHash } from "node:crypto";
import type { Config } from "../config";
import type { ChatRequest } from "../types/chat";

function hash(value: unknown): string {
  // Object property order is irrelevant; array order and text bytes are not.
  const canonical = JSON.stringify(value, (_key, item: unknown) => {
    if (item !== null && typeof item === "object" && !Array.isArray(item)) {
      return Object.fromEntries(Object.entries(item).sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0));
    }
    return item;
  });
  return createHash("sha256").update(canonical).digest("hex");
}

export function chatCacheKey(namespace: string, ownerId: string, request: ChatRequest): string {
  // Hash the complete validated request, not only the latest user message.
  return `modelmux:chat-cache:v1:${hash([namespace, ownerId, request])}`;
}

export function chatCacheNamespace(config: Config): string {
  return hash({
    version: config.chatCache.version,
    ollamaUrl: config.ollamaBaseUrl,
    ollamaTimeoutMs: config.ollamaRequestTimeoutMs,
    circuit: config.circuitBreaker,
    fallback: config.openai ? {
      modelMap: config.openai.modelMap,
      timeoutMs: config.openai.requestTimeoutMs,
      credentialHash: hash(config.openai.apiKey),
    } : null,
  });
}
