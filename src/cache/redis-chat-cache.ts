import type { Redis } from "ioredis";
import { abortable, RequestAbortedError, throwIfCancelled } from "../cancellation";
import type { ChatRequest, ChatResponse } from "../types/chat";
import type { ChatCache } from "./chat-cache";
import { chatCacheKey } from "./cache-key";

export interface RedisChatCacheOptions {
  namespace: string;
  ttlSeconds: number;
  maxEntryBytes: number;
  timeoutMs: number;
}

export class RedisChatCache implements ChatCache {
  public constructor(
    private readonly redis: Pick<Redis, "getrangeBuffer" | "set">,
    private readonly options: RedisChatCacheOptions,
  ) {
    for (const name of ["ttlSeconds", "maxEntryBytes", "timeoutMs"] as const) {
      if (!Number.isSafeInteger(options[name]) || options[name] < 1 || options[name] > 2_147_483_647) {
        throw new Error(`Cache ${name} must be a positive integer`);
      }
    }
    if (!options.namespace) throw new Error("Cache namespace is required");
  }

  public async get(ownerId: string, request: ChatRequest, signal: AbortSignal): Promise<ChatResponse | null> {
    const key = chatCacheKey(this.options.namespace, ownerId, request);
    // GETRANGE's end is inclusive: one extra byte detects oversized entries
    // without downloading an arbitrarily large value from Redis.
    const data = await this.command(() => this.redis.getrangeBuffer(key, 0, this.options.maxEntryBytes), signal);
    if (data.length === 0 || data.length > this.options.maxEntryBytes) return null;
    let value: unknown;
    try {
      value = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(data));
    } catch {
      return null;
    }
    return isChatResponse(value) ? { message: { role: "assistant", content: value.message.content } } : null;
  }

  public async put(ownerId: string, request: ChatRequest, response: ChatResponse, signal: AbortSignal): Promise<void> {
    throwIfCancelled(signal);
    if (!isChatResponse(response)) return;
    if (Buffer.byteLength(response.message.content, "utf8") > this.options.maxEntryBytes) return;
    // Exclude request IDs, headers, provider metadata, and any extra properties.
    const value = JSON.stringify({ message: { role: "assistant", content: response.message.content } });
    if (Buffer.byteLength(value, "utf8") > this.options.maxEntryBytes) return;
    const key = chatCacheKey(this.options.namespace, ownerId, request);
    // Atomic value + expiry. First completed answer wins concurrent misses;
    // neither hits nor racing completions extend the original entry's lifetime.
    await this.command(() => this.redis.set(key, value, "EX", this.options.ttlSeconds, "NX"), signal);
  }

  private async command<T>(operation: () => Promise<T>, parent: AbortSignal): Promise<T> {
    throwIfCancelled(parent);
    const controller = new AbortController();
    const cancel = () => controller.abort(new RequestAbortedError());
    parent.addEventListener("abort", cancel, { once: true });
    const timer = setTimeout(() => controller.abort(new Error("Chat cache timed out")), this.options.timeoutMs);
    try {
      const result = await abortable(operation(), controller.signal);
      controller.signal.throwIfAborted();
      return result;
    } finally {
      clearTimeout(timer);
      parent.removeEventListener("abort", cancel);
    }
  }
}

function isChatResponse(value: unknown): value is ChatResponse {
  if (value === null || typeof value !== "object" || !("message" in value)) return false;
  const message = value.message;
  return message !== null && typeof message === "object"
    && "role" in message && message.role === "assistant"
    && "content" in message && typeof message.content === "string";
}
