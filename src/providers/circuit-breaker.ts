import { performance } from "node:perf_hooks";

export type CircuitState = "closed" | "open" | "half-open";

export interface CircuitBreakerOptions {
  failureThreshold: number;
  resetTimeoutMs: number;
  now?: () => number;
}

export interface CircuitSnapshot {
  state: CircuitState;
  consecutiveFailures: number;
  retryAfterMs: number;
}

export class CircuitOpenError extends Error {
  public constructor(public readonly retryAfterMs: number) {
    super("Provider circuit is open");
    this.name = "CircuitOpenError";
  }
}

/** Process-local failure tracking. Recovery uses one real request as a probe;
 * there are no timers or background provider calls.
 */
export class CircuitBreaker {
  private state: CircuitState = "closed";
  private consecutiveFailures = 0;
  private openedAt = 0;
  private generation = 0;
  private readonly failureThreshold: number;
  private readonly resetTimeoutMs: number;
  private readonly now: () => number;

  public constructor(options: CircuitBreakerOptions) {
    if (!Number.isSafeInteger(options.failureThreshold) || options.failureThreshold < 1) {
      throw new Error("failureThreshold must be a positive safe integer");
    }

    if (!Number.isSafeInteger(options.resetTimeoutMs) || options.resetTimeoutMs < 1) {
      throw new Error("resetTimeoutMs must be a positive safe integer");
    }

    if (options.now !== undefined && typeof options.now !== "function") {
      throw new Error("now must be a function");
    }

    this.failureThreshold = options.failureThreshold;
    this.resetTimeoutMs = options.resetTimeoutMs;
    this.now = options.now ?? (() => performance.now());
  }

  public async execute<T>(
    operation: () => Promise<T>,
    isFailure: (error: unknown) => boolean,
  ): Promise<T> {
    const snapshot = this.snapshot();

    if (snapshot.state === "half-open" || snapshot.retryAfterMs > 0) {
      throw new CircuitOpenError(snapshot.retryAfterMs);
    }

    if (this.state === "open") {
      // Reserve the probe before awaiting so concurrent callers cannot enter.
      this.state = "half-open";
    }

    const generation = this.generation;

    try {
      const result = await operation();

      if (generation === this.generation) {
        this.recordHealthy();
      }

      return result;
    } catch (error) {
      if (generation === this.generation) {
        if (isFailure(error)) {
          this.recordFailure();
        } else {
          // A permanent request error still demonstrates provider reachability.
          // It resets failures (and closes a probe) but is rethrown to the caller.
          this.recordHealthy();
        }
      }

      throw error;
    }
  }

  public snapshot(): CircuitSnapshot {
    return {
      state: this.state,
      consecutiveFailures: this.consecutiveFailures,
      retryAfterMs: this.state === "open"
        ? Math.max(0, this.resetTimeoutMs - (this.now() - this.openedAt))
        // A pending probe has no known completion time; use the cooldown as a hint.
        : this.state === "half-open" ? this.resetTimeoutMs : 0,
    };
  }

  private recordHealthy(): void {
    if (this.state === "half-open") {
      this.generation += 1;
    }

    this.state = "closed";
    this.consecutiveFailures = 0;
  }

  private recordFailure(): void {
    this.consecutiveFailures += 1;

    if (this.state === "half-open" || this.consecutiveFailures >= this.failureThreshold) {
      this.state = "open";
      this.openedAt = this.now();
      // Ignore completions from calls admitted before this circuit opened.
      this.generation += 1;
    }
  }
}
