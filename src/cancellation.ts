// Client cancellation is not evidence that a provider is unhealthy.
export class RequestAbortedError extends Error {
  public constructor() {
    super("Request cancelled");
    this.name = "RequestAbortedError";
  }
}

export function throwIfCancelled(signal?: AbortSignal): void {
  if (signal?.aborted) throw new RequestAbortedError();
}

// Also interrupts waits on downstream backpressure. The underlying operation
// must still receive the signal so its I/O is actually cancelled.
export function abortable<T>(operation: Promise<T>, signal: AbortSignal): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const onAbort = () => {
      signal.removeEventListener("abort", onAbort);
      reject(signal.reason);
    };
    signal.addEventListener("abort", onAbort, { once: true });
    if (signal.aborted) onAbort();
    operation.then(resolve, reject).finally(() => signal.removeEventListener("abort", onAbort));
  });
}
