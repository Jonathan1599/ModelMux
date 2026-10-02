import {
  ProviderConnectionError,
  ProviderHttpError,
  ProviderResponseError,
  ProviderTimeoutError,
} from "../errors";
import type { ChatRequest, ChatResponse } from "../types/chat";
import type { LLMProvider } from "./provider";

export class OpenAIProvider implements LLMProvider {
  public constructor(
    private readonly apiKey: string,
    private readonly requestTimeoutMs = 30_000,
  ) {
    if (!/^[\x21-\x7E]+$/.test(apiKey)) {
      throw new Error("OpenAI API key must be a non-empty token without whitespace");
    }

    if (
      !Number.isInteger(requestTimeoutMs) ||
      requestTimeoutMs < 1 ||
      requestTimeoutMs > 2_147_483_647
    ) {
      throw new Error("OpenAI request timeout must be a positive integer");
    }
  }

  public async chat(request: ChatRequest): Promise<ChatResponse> {
    const signal = AbortSignal.timeout(this.requestTimeoutMs);
    let response: Response;

    try {
      response = await fetch("https://api.openai.com/v1/chat/completions", {
        method: "POST",
        headers: {
          authorization: `Bearer ${this.apiKey}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          model: request.model,
          messages: request.messages,
          stream: false,
          store: false,
        }),
        redirect: "error",
        signal,
      });
    } catch {
      if (signal.aborted) {
        throw new ProviderTimeoutError("OpenAI", this.requestTimeoutMs);
      }

      throw new ProviderConnectionError("OpenAI");
    }

    if (!response.ok) {
      // Discard upstream errors: their bodies can echo credentials or prompts.
      void response.body?.cancel().catch(() => undefined);
      throw new ProviderHttpError("OpenAI", response.status, undefined);
    }

    let data: unknown;

    try {
      data = await response.json();
    } catch {
      if (signal.aborted) {
        throw new ProviderTimeoutError("OpenAI", this.requestTimeoutMs);
      }

      // JSON parse errors can include the response body in their message.
      throw new ProviderResponseError("OpenAI");
    }

    if (signal.aborted) {
      throw new ProviderTimeoutError("OpenAI", this.requestTimeoutMs);
    }

    if (!isRecord(data) || !Array.isArray(data.choices)) {
      throw new ProviderResponseError("OpenAI");
    }

    const choice: unknown = data.choices[0];
    const message = isRecord(choice) ? choice.message : undefined;

    if (!isRecord(message) || message.role !== "assistant") {
      throw new ProviderResponseError("OpenAI");
    }

    let content: string;

    if (typeof message.content === "string") {
      content = message.content;
    } else if (
      (message.content === null || message.content === undefined) &&
      typeof message.refusal === "string"
    ) {
      // A refusal is a completed answer, not a provider failure to retry.
      content = message.refusal;
    } else {
      throw new ProviderResponseError("OpenAI");
    }

    return { message: { role: "assistant", content } };
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
