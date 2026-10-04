import { ProviderConnectionError, ProviderHttpError, ProviderResponseError } from "../errors";
import type { ChatRequest, ChatResponse } from "../types/chat";
import type { ChatDeltaConsumer, LLMProvider, ProviderContext } from "./provider";
import { withProviderDeadline } from "./http-request";
import { isRecord, parseStreamJson, readSseData } from "./stream-parser";

export class OpenAIProvider implements LLMProvider {
  public constructor(private readonly apiKey: string, private readonly requestTimeoutMs = 30_000) {
    if (!/^[\x21-\x7E]+$/.test(apiKey)) {
      throw new Error("OpenAI API key must be a non-empty token without whitespace");
    }
    if (!Number.isInteger(requestTimeoutMs) || requestTimeoutMs < 1 || requestTimeoutMs > 2_147_483_647) {
      throw new Error("OpenAI request timeout must be a positive integer");
    }
  }

  public chat(request: ChatRequest, context?: ProviderContext): Promise<ChatResponse> {
    return withProviderDeadline("OpenAI", this.requestTimeoutMs, context?.signal, async (signal) => {
      const response = await this.send(request, false, signal);
      let data: unknown;
      try { data = await response.json(); } catch {
        signal.throwIfAborted();
        throw new ProviderResponseError("OpenAI");
      }
      if (!isRecord(data) || !Array.isArray(data.choices)) throw new ProviderResponseError("OpenAI");
      const choice: unknown = data.choices[0];
      const message = isRecord(choice) ? choice.message : undefined;
      if (!isRecord(message) || message.role !== "assistant") throw new ProviderResponseError("OpenAI");
      return { message: { role: "assistant", content: textContent(message) } };
    });
  }

  public stream(request: ChatRequest, onDelta: ChatDeltaConsumer, context?: ProviderContext): Promise<void> {
    return withProviderDeadline("OpenAI", this.requestTimeoutMs, context?.signal, async (signal) => {
      const response = await this.send(request, true, signal);
      let finished = false;
      for await (const event of readSseData(response, signal, "OpenAI")) {
        signal.throwIfAborted();
        if (event === "[DONE]") {
          if (!finished) throw new ProviderResponseError("OpenAI");
          return;
        }
        const data = parseStreamJson(event, "OpenAI");
        if (data.error !== undefined || !Array.isArray(data.choices)) throw new ProviderResponseError("OpenAI");
        if (data.choices.length === 0) continue; // Optional usage-only event.
        const choice: unknown = data.choices[0];
        if (finished || !isRecord(choice) || !isRecord(choice.delta)
          || (choice.index !== undefined && choice.index !== 0)) throw new ProviderResponseError("OpenAI");
        const delta = choice.delta;
        if ((delta.role !== undefined && delta.role !== "assistant")
          || delta.tool_calls !== undefined || delta.function_call !== undefined) {
          throw new ProviderResponseError("OpenAI");
        }
        const content = delta.content == null && delta.refusal == null ? "" : textContent(delta);
        if (content) await onDelta(content);
        if (choice.finish_reason != null) {
          if (!["stop", "length", "content_filter"].includes(String(choice.finish_reason))) {
            throw new ProviderResponseError("OpenAI");
          }
          finished = true;
        }
      }
      throw new ProviderResponseError("OpenAI"); // Require the explicit terminal event.
    });
  }

  private async send(request: ChatRequest, stream: boolean, signal: AbortSignal): Promise<Response> {
    signal.throwIfAborted();
    let response: Response;
    try {
      response = await fetch("https://api.openai.com/v1/chat/completions", {
        method: "POST",
        headers: { authorization: `Bearer ${this.apiKey}`, "content-type": "application/json" },
        body: JSON.stringify({ model: request.model, messages: request.messages, stream, store: false }),
        redirect: "error",
        signal,
      });
    } catch {
      signal.throwIfAborted();
      throw new ProviderConnectionError("OpenAI");
    }
    if (!response.ok) {
      void response.body?.cancel().catch(() => undefined);
      throw new ProviderHttpError("OpenAI", response.status, undefined);
    }
    return response;
  }
}

function textContent(message: Record<string, unknown>): string {
  if (typeof message.content === "string") return message.content;
  if (message.content == null && typeof message.refusal === "string") return message.refusal;
  throw new ProviderResponseError("OpenAI");
}
