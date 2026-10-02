import { AppError, ProviderCircuitOpenError } from "../errors";
import type { ChatRequest, ChatResponse } from "../types/chat";
import { CircuitBreaker, CircuitOpenError, type CircuitSnapshot } from "./circuit-breaker";
import { isRetryableProviderError } from "./failure-policy";
import type { LLMProvider, ProviderContext } from "./provider";

export interface ProviderTarget {
  name: string;
  provider: LLMProvider;
  circuit: CircuitBreaker;
}

export interface FallbackTarget extends ProviderTarget {
  // Explicit mappings avoid sending arbitrary local model names to a host.
  modelMap: Readonly<Record<string, string>>;
}

export interface ProviderHealth extends CircuitSnapshot {
  name: string;
}

export class ProviderRouter implements LLMProvider {
  public constructor(
    private readonly primary: ProviderTarget,
    private readonly fallback?: FallbackTarget,
  ) {}

  public snapshot(): ProviderHealth[] {
    const targets = this.fallback ? [this.primary, this.fallback] : [this.primary];
    return targets.map(({ name, circuit }) => ({ name, ...circuit.snapshot() }));
  }

  public async chat(request: ChatRequest, context?: ProviderContext): Promise<ChatResponse> {
    try {
      return await this.call(this.primary, request, context);
    } catch (error) {
      const fallback = this.fallback;
      if (!isRetryableProviderError(error) || !fallback
        || !Object.hasOwn(fallback.modelMap, request.model)) {
        throw error;
      }
      const model = fallback.modelMap[request.model];
      if (!model) throw error;

      context?.logger.warn({
        primary: this.primary.name, fallback: fallback.name, code: error.code,
      }, "Trying fallback provider");

      // Each target gets at most one call per routing pass. Durable job retries
      // remain the worker's responsibility. The last target's error is returned.
      return this.call(fallback, { ...request, model }, context);
    }
  }

  private async call(
    target: ProviderTarget,
    request: ChatRequest,
    context?: ProviderContext,
  ): Promise<ChatResponse> {
    const startedAt = performance.now();
    try {
      const response = await target.circuit.execute(
        () => target.provider.chat(request, context), isRetryableProviderError,
      );
      context?.logger.info({
        provider: target.name,
        providerLatencyMs: performance.now() - startedAt,
        circuitState: target.circuit.snapshot().state,
      }, "Provider call completed");
      return response;
    } catch (cause) {
      const error = cause instanceof CircuitOpenError
        ? new ProviderCircuitOpenError(target.name, cause.retryAfterMs) : cause;
      context?.logger.warn({
        provider: target.name,
        code: error instanceof AppError ? error.code : "UNEXPECTED_PROVIDER_ERROR",
        circuitState: target.circuit.snapshot().state,
      }, "Provider call failed or skipped");
      throw error;
    }
  }
}
