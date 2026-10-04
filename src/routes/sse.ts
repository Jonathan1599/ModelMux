import type { ServerResponse } from "node:http";
import { RequestAbortedError, throwIfCancelled } from "../cancellation";

// Stop reading upstream when the socket buffer is full. Bound the wait so a
// client that stops reading cannot hold a provider slot indefinitely.
export function writeSse(
  response: ServerResponse,
  event: "delta" | "done" | "error",
  data: unknown,
  signal: AbortSignal,
  writeTimeoutMs = 15_000,
): Promise<void> {
  throwIfCancelled(signal);
  return new Promise<void>((resolve, reject) => {
    let settled = false;
    let timer: NodeJS.Timeout | undefined;
    const finish = (error?: Error) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      response.off("drain", onDrain);
      response.off("close", onClose);
      response.off("error", onClose);
      signal.removeEventListener("abort", onClose);
      if (error) reject(error); else resolve();
    };
    const onDrain = () => finish();
    const onClose = () => finish(new RequestAbortedError());
    response.once("drain", onDrain);
    response.once("close", onClose);
    response.once("error", onClose);
    signal.addEventListener("abort", onClose, { once: true });
    if (response.destroyed || signal.aborted) return onClose();
    try {
      if (response.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`)) finish();
      else if (!settled) timer = setTimeout(onClose, writeTimeoutMs);
    } catch { onClose(); }
  });
}
