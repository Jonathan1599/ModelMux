import { AppError, ProviderCircuitOpenError, ProviderStreamingUnsupportedError } from "../errors";
import { RequestAbortedError, throwIfCancelled } from "../cancellation";
import type { ChatRequest, ChatResponse } from "../types/chat";
import { CircuitBreaker, CircuitOpenError, type CircuitSnapshot } from "./circuit-breaker";
import { isRetryableProviderError } from "./failure-policy";
import type { ChatDeltaConsumer, LLMProvider, ProviderContext } from "./provider";
import { recordMetric, type MetricSink, type ProviderOutcome } from "../observability/telemetry";

class StreamConsumerError extends Error {
  public constructor() { super("Stream consumer failed"); }
}

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
    private readonly metrics?: MetricSink,
  ) {}

  public snapshot(): ProviderHealth[] {
    const targets = this.fallback ? [this.primary, this.fallback] : [this.primary];
    return targets.map(({ name, circuit }) => ({ name, ...circuit.snapshot() }));
  }

  public chat(request: ChatRequest, context?: ProviderContext): Promise<ChatResponse> {
    return this.route(request, (provider, data) => provider.chat(data, context), context);
  }

  public stream(request: ChatRequest, onDelta: ChatDeltaConsumer, context?: ProviderContext): Promise<void> {
    let emitted = false;
    const consume: ChatDeltaConsumer = async (content) => {
      throwIfCancelled(context?.signal);
      if (!content) return;
      emitted = true; // Never switch models once a write has been attempted.
      try { await onDelta(content); } catch {
        throwIfCancelled(context?.signal);
        throw new StreamConsumerError();
      }
    };
    return this.route(request, (provider, data) => {
      if (!provider.stream) throw new ProviderStreamingUnsupportedError();
      return provider.stream(data, consume, context);
    }, context, () => !emitted, "stream");
  }

  private async route<T>(
    request: ChatRequest,
    invoke: (provider: LLMProvider, data: ChatRequest) => Promise<T>,
    context?: ProviderContext,
    canFallback: () => boolean = () => true,
    mode: "chat" | "stream" = "chat",
  ): Promise<T> {
    throwIfCancelled(context?.signal);
    try {
      return await this.call(this.primary, () => invoke(this.primary.provider, request), context, mode);
    } catch (error) {
      throwIfCancelled(context?.signal);
      const fallback = this.fallback;
      if (!canFallback() || !isRetryableProviderError(error) || !fallback
        || !Object.hasOwn(fallback.modelMap, request.model)) {
        throw error;
      }
      const model = fallback.modelMap[request.model];
      if (!model) throw error;
      recordMetric(this.metrics, { kind: "fallback", from: this.primary.name, to: fallback.name });

      context?.logger?.warn({
        primary: this.primary.name, fallback: fallback.name, code: error.code,
      }, "Trying fallback provider");

      // Each target gets at most one call per routing pass. Durable job retries
      // remain the worker's responsibility. The last target's error is returned.
      return this.call(fallback, () => invoke(fallback.provider, { ...request, model }), context, mode);
    }
  }

  private async call<T>(
    target: ProviderTarget,
    operation: () => Promise<T>,
    context?: ProviderContext,
    mode: "chat" | "stream" = "chat",
  ): Promise<T> {
    const startedAt = performance.now();
    let attempted = false;
    let outcome: ProviderOutcome = "success";
    try {
      const response = await target.circuit.execute(
        async () => {
          throwIfCancelled(context?.signal);
          attempted = true;
          recordMetric(this.metrics, { kind: "provider_start", provider: target.name, mode });
          try {
            const result = await operation();
            throwIfCancelled(context?.signal);
            return result;
          } catch (error) {
            throwIfCancelled(context?.signal);
            throw error;
          }
        },
        isRetryableProviderError,
        (error) => error instanceof RequestAbortedError || error instanceof StreamConsumerError,
      );
      context?.logger?.info({
        provider: target.name,
        providerLatencyMs: performance.now() - startedAt,
        circuitState: target.circuit.snapshot().state,
      }, "Provider call completed");
      return response;
    } catch (cause) {
      outcome = cause instanceof RequestAbortedError ? "cancelled"
        : cause instanceof StreamConsumerError ? "consumer_error"
        : isRetryableProviderError(cause) ? "transient_error" : "permanent_error";
      if (cause instanceof CircuitOpenError) {
        recordMetric(this.metrics, { kind: "circuit_rejection", provider: target.name, mode });
      }
      const error = cause instanceof CircuitOpenError
        ? new ProviderCircuitOpenError(target.name, cause.retryAfterMs) : cause;
      context?.logger?.warn({
        provider: target.name,
        code: error instanceof RequestAbortedError ? "REQUEST_ABORTED"
          : error instanceof AppError ? error.code : "UNEXPECTED_PROVIDER_ERROR",
        circuitState: target.circuit.snapshot().state,
      }, "Provider call failed or skipped");
      throw error;
    } finally {
      if (attempted) recordMetric(this.metrics, {
        kind: "provider_end", provider: target.name, mode, outcome, durationMs: performance.now() - startedAt,
      });
    }
  }
}
