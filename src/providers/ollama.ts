import { ProviderConnectionError, ProviderHttpError, ProviderResponseError } from "../errors";
import type { ChatRequest, ChatResponse } from "../types/chat";
import type { ChatDeltaConsumer, LLMProvider, ProviderContext } from "./provider";
import { withProviderDeadline } from "./http-request";
import { isRecord, parseStreamJson, readLines } from "./stream-parser";

export class OllamaProvider implements LLMProvider {
  private readonly chatUrl: URL;

  public constructor(baseUrl: string, private readonly requestTimeoutMs = 120_000) {
    if (!Number.isInteger(requestTimeoutMs) || requestTimeoutMs < 1 || requestTimeoutMs > 2_147_483_647) {
      throw new Error("Ollama request timeout must be a positive integer");
    }
    const normalizedBaseUrl = baseUrl.endsWith("/") ? baseUrl : `${baseUrl}/`;
    try { this.chatUrl = new URL("api/chat", normalizedBaseUrl); }
    catch (cause) { throw new Error(`Invalid Ollama base URL: ${baseUrl}`, { cause }); }
  }

  public chat(request: ChatRequest, context?: ProviderContext): Promise<ChatResponse> {
    return withProviderDeadline("Ollama", this.requestTimeoutMs, context?.signal, async (signal) => {
      const response = await this.send(request, false, signal);
      let data: unknown;
      try { data = await response.json(); } catch {
        signal.throwIfAborted();
        throw new ProviderResponseError("Ollama");
      }
      if (!isRecord(data) || !isRecord(data.message) || typeof data.message.content !== "string") {
        throw new ProviderResponseError("Ollama");
      }
      return { message: { role: "assistant", content: data.message.content } };
    });
  }

  public stream(request: ChatRequest, onDelta: ChatDeltaConsumer, context?: ProviderContext): Promise<void> {
    return withProviderDeadline("Ollama", this.requestTimeoutMs, context?.signal, async (signal) => {
      const response = await this.send(request, true, signal);
      for await (const line of readLines(response, signal, "Ollama")) {
        if (!line.trim()) continue;
        signal.throwIfAborted();
        const data = parseStreamJson(line, "Ollama");
        if (data.error !== undefined || typeof data.done !== "boolean") {
          throw new ProviderResponseError("Ollama");
        }
        if (data.message !== undefined) {
          if (!isRecord(data.message) || typeof data.message.content !== "string"
            || (data.message.role !== undefined && data.message.role !== "assistant")
            || (Array.isArray(data.message.tool_calls) && data.message.tool_calls.length > 0)) {
            throw new ProviderResponseError("Ollama");
          }
          if (data.message.content) await onDelta(data.message.content);
        } else if (!data.done) {
          throw new ProviderResponseError("Ollama");
        }
        if (data.done) return;
      }
      throw new ProviderResponseError("Ollama"); // EOF without done is truncated.
    });
  }

  private async send(request: ChatRequest, stream: boolean, signal: AbortSignal): Promise<Response> {
    signal.throwIfAborted();
    let response: Response;
    try {
      response = await fetch(this.chatUrl, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ model: request.model, messages: request.messages, stream }),
        signal,
      });
    } catch (cause) {
      signal.throwIfAborted();
      throw new ProviderConnectionError("Ollama", cause);
    }
    if (!response.ok) {
      const detail = await readErrorDetail(response);
      signal.throwIfAborted();
      throw new ProviderHttpError("Ollama", response.status, detail);
    }
    return response;
  }
}

async function readErrorDetail(response: Response): Promise<string | undefined> {
  try {
    const body: unknown = await response.json();
    return isRecord(body) && typeof body.error === "string" ? body.error.slice(0, 300) : undefined;
  } catch { return undefined; }
}
