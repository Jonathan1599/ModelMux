import { abortable, RequestAbortedError, throwIfCancelled } from "../cancellation";
import { ProviderTimeoutError } from "../errors";

export async function withProviderDeadline<T>(
  provider: string,
  timeoutMs: number,
  parent: AbortSignal | undefined,
  operation: (signal: AbortSignal) => Promise<T>,
): Promise<T> {
  throwIfCancelled(parent);
  const controller = new AbortController();
  const cancel = () => controller.abort(new RequestAbortedError());
  parent?.addEventListener("abort", cancel, { once: true });
  const timer = setTimeout(() => {
    controller.abort(new ProviderTimeoutError(provider, timeoutMs));
  }, timeoutMs);
  timer.unref();
  try {
    const result = await abortable(operation(controller.signal), controller.signal);
    controller.signal.throwIfAborted();
    return result;
  } finally {
    clearTimeout(timer);
    parent?.removeEventListener("abort", cancel);
    // Terminate unread response bodies on errors or early protocol completion.
    controller.abort();
  }
}
