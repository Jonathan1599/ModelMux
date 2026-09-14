import {
  ProviderConnectionError,
  ProviderHttpError,
  ProviderResponseError,
  ProviderTimeoutError,
} from "../errors";
import type { ChatRequest, ChatResponse } from "../types/chat";
import type { LLMProvider } from "./provider";

interface OllamaChatResponse {
  message?: {
    role?: string;
    content?: string;
  };
}

export class OllamaProvider implements LLMProvider {
  private readonly chatUrl: URL;

  public constructor(
    baseUrl: string,
    private readonly requestTimeoutMs = 120_000,
  ) {
    const normalizedBaseUrl = baseUrl.endsWith("/") ? baseUrl : `${baseUrl}/`;

    if (
      !Number.isInteger(requestTimeoutMs) ||
      requestTimeoutMs < 1 ||
      requestTimeoutMs > 2_147_483_647
    ) {
      throw new Error("Ollama request timeout must be a positive integer");
    }

    try {
      this.chatUrl = new URL("api/chat", normalizedBaseUrl);
    } catch (cause) {
      throw new Error(`Invalid Ollama base URL: ${baseUrl}`, { cause });
    }
  }

  public async chat(request: ChatRequest): Promise<ChatResponse> {
    let response: Response;
    const signal = AbortSignal.timeout(this.requestTimeoutMs);

    try {
      response = await fetch(this.chatUrl, {
        method: "POST",
        headers: {
          "content-type": "application/json",
        },
        body: JSON.stringify({
          model: request.model,
          messages: request.messages,
          stream: false,
        }),
        signal,
      });
    } catch (cause) {
      if (signal.aborted) {
        throw new ProviderTimeoutError("Ollama", this.requestTimeoutMs, cause);
      }

      throw new ProviderConnectionError("Ollama", cause);
    }

    if (!response.ok) {
      const detail = await readErrorDetail(response);

      if (signal.aborted) {
        throw new ProviderTimeoutError("Ollama", this.requestTimeoutMs, signal.reason);
      }

      throw new ProviderHttpError("Ollama", response.status, detail);
    }

    let data: OllamaChatResponse;

    try {
      data = (await response.json()) as OllamaChatResponse;
    } catch (cause) {
      if (signal.aborted) {
        throw new ProviderTimeoutError("Ollama", this.requestTimeoutMs, cause);
      }

      throw new ProviderResponseError("Ollama", cause);
    }

    if (typeof data.message?.content !== "string") {
      throw new ProviderResponseError("Ollama");
    }

    return {
      message: {
        role: "assistant",
        content: data.message.content,
      },
    };
  }
}

async function readErrorDetail(response: Response): Promise<string | undefined> {
  try {
    const body = (await response.json()) as { error?: unknown };
    return typeof body.error === "string" ? body.error.slice(0, 300) : undefined;
  } catch {
    return undefined;
  }
}
