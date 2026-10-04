import { performance } from "node:perf_hooks";
import { RequestAbortedError } from "../cancellation";
import {
  ConcurrencyQueueFullError,
  ConcurrencyWaitTimeoutError,
} from "../errors";
import type {
  ConcurrencyLimiter,
  ConcurrencyPermit,
  ConcurrencySnapshot,
} from "./concurrency-limiter";

interface PendingAcquire {
  enqueuedAt: number;
  resolve: (permit: ConcurrencyPermit) => void;
  timer: NodeJS.Timeout | undefined;
  cleanup: () => void;
}

export interface InMemoryConcurrencyLimiterOptions {
  maxConcurrent: number;
  maxQueueSize: number;
  waitTimeoutMs: number;
}

/* Process-local semaphore for provider calls. Requests run immediately while a
slot is available; excess requests wait in a bounded FIFO queue and time out
if capacity does not open soon enough. Releasing a permit hands its slot
directly to the oldest waiter, and each permit can be released only once.
*/

export class InMemoryConcurrencyLimiter implements ConcurrencyLimiter {
  private active = 0;
  private readonly queue: PendingAcquire[] = [];
  private admitted = 0;
  private rejected = 0;
  private timedOut = 0;
  private aborted = 0;
  private totalWaitTimeMs = 0;
  private maxWaitTimeMs = 0;

  public constructor(
    private readonly options: InMemoryConcurrencyLimiterOptions,
  ) {
    validateOptions(options);
  }

  public async acquire(signal?: AbortSignal): Promise<ConcurrencyPermit> {
    if (signal?.aborted) {
      this.aborted += 1;
      throw new RequestAbortedError();
    }
    if (this.active < this.options.maxConcurrent) {
      this.active += 1;
      return this.admit(0);
    }

    if (this.queue.length >= this.options.maxQueueSize) {
      this.rejected += 1;
      throw new ConcurrencyQueueFullError(
        this.options.maxConcurrent,
        this.options.maxQueueSize,
      );
    }

    return new Promise<ConcurrencyPermit>((resolve, reject) => {
      const pending: PendingAcquire = {
        enqueuedAt: performance.now(),
        resolve,
        timer: undefined,
        cleanup: () => {
          clearTimeout(pending.timer);
          signal?.removeEventListener("abort", onAbort);
        },
      };

      const remove = (): boolean => {
        const index = this.queue.indexOf(pending);
        if (index === -1) return false;
        this.queue.splice(index, 1);
        pending.cleanup();
        return true;
      };
      const onAbort = () => {
        if (!remove()) return;
        this.aborted += 1;
        reject(new RequestAbortedError());
      };

      pending.timer = setTimeout(() => {
        if (!remove()) return;
        this.timedOut += 1;
        reject(new ConcurrencyWaitTimeoutError(this.options.waitTimeoutMs));
      }, this.options.waitTimeoutMs);

      this.queue.push(pending);
      signal?.addEventListener("abort", onAbort, { once: true });
    });
  }

  public snapshot(): ConcurrencySnapshot {
    return {
      active: this.active,
      queued: this.queue.length,
      admitted: this.admitted,
      rejected: this.rejected,
      timedOut: this.timedOut,
      aborted: this.aborted,
      totalWaitTimeMs: this.totalWaitTimeMs,
      maxWaitTimeMs: this.maxWaitTimeMs,
    };
  }

  private admit(waitTimeMs: number): ConcurrencyPermit {
    this.admitted += 1;
    this.totalWaitTimeMs += waitTimeMs;
    this.maxWaitTimeMs = Math.max(this.maxWaitTimeMs, waitTimeMs);
    let released = false;

    return {
      waitTimeMs,
      activeAtAdmission: this.active,
      queuedAtAdmission: this.queue.length,
      release: () => {
        if (released) {
          return;
        }

        released = true;
        this.release();
      },
    };
  }

  private release(): void {
    const next = this.queue.shift();

    if (!next) {
      this.active -= 1;
      return;
    }

    next.cleanup();

    const waitTimeMs = performance.now() - next.enqueuedAt;
    next.resolve(this.admit(waitTimeMs));
  }
}

function validateOptions(options: InMemoryConcurrencyLimiterOptions): void {
  if (!Number.isInteger(options.maxConcurrent) || options.maxConcurrent < 1) {
    throw new Error("maxConcurrent must be a positive integer");
  }

  if (!Number.isInteger(options.maxQueueSize) || options.maxQueueSize < 0) {
    throw new Error("maxQueueSize must be a non-negative integer");
  }

  if (!Number.isInteger(options.waitTimeoutMs) || options.waitTimeoutMs < 1) {
    throw new Error("waitTimeoutMs must be a positive integer");
  }
}
