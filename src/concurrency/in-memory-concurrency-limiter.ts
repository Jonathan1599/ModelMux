import { performance } from "node:perf_hooks";
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
  reject: (error: ConcurrencyWaitTimeoutError) => void;
  timer: NodeJS.Timeout | undefined;
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
  private totalWaitTimeMs = 0;
  private maxWaitTimeMs = 0;

  public constructor(
    private readonly options: InMemoryConcurrencyLimiterOptions,
  ) {
    validateOptions(options);
  }

  public async acquire(): Promise<ConcurrencyPermit> {
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
        reject,
        timer: undefined,
      };

      pending.timer = setTimeout(() => {
        const index = this.queue.indexOf(pending);

        if (index === -1) {
          return;
        }

        this.queue.splice(index, 1);
        this.timedOut += 1;
        reject(new ConcurrencyWaitTimeoutError(this.options.waitTimeoutMs));
      }, this.options.waitTimeoutMs);

      this.queue.push(pending);
    });
  }

  public snapshot(): ConcurrencySnapshot {
    return {
      active: this.active,
      queued: this.queue.length,
      admitted: this.admitted,
      rejected: this.rejected,
      timedOut: this.timedOut,
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

    if (next.timer) {
      clearTimeout(next.timer);
    }

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
