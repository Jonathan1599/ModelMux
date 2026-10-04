import { abortable } from "../cancellation";
import { ProviderConnectionError, ProviderResponseError } from "../errors";

const MAX_FRAME_SIZE = 65_536;

export function parseStreamJson(value: string, provider: string): Record<string, unknown> {
  let data: unknown;
  try { data = JSON.parse(value); } catch { throw new ProviderResponseError(provider); }
  if (!isRecord(data)) throw new ProviderResponseError(provider);
  return data;
}

export function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

// Decode incrementally: network boundaries need not align with UTF-8 or lines.
// Only an unfinished line plus the current network chunk are held in memory.
export async function* readLines(
  response: Response, signal: AbortSignal, provider: string,
): AsyncGenerator<string> {
  if (!response.body) throw new ProviderResponseError(provider);
  const reader = response.body.getReader();
  const decoder = new TextDecoder("utf-8", { fatal: true });
  let pending = "";
  try {
    while (true) {
      signal.throwIfAborted();
      let part: ReadableStreamReadResult<Uint8Array>;
      try { part = await abortable(reader.read(), signal); } catch {
        signal.throwIfAborted();
        throw new ProviderConnectionError(provider);
      }
      let decoded: string;
      try { decoded = part.done ? decoder.decode() : decoder.decode(part.value, { stream: true }); }
      catch { throw new ProviderResponseError(provider); }
      let start = 0;
      for (let index = 0; index < decoded.length; index += 1) {
        if (decoded[index] !== "\n") continue;
        pending += decoded.slice(start, index);
        if (pending.length > MAX_FRAME_SIZE) throw new ProviderResponseError(provider);
        yield pending.endsWith("\r") ? pending.slice(0, -1) : pending;
        pending = "";
        start = index + 1;
      }
      pending += decoded.slice(start);
      if (pending.length > MAX_FRAME_SIZE) throw new ProviderResponseError(provider);
      if (part.done) break;
    }
    if (pending) yield pending.endsWith("\r") ? pending.slice(0, -1) : pending;
  } finally {
    void reader.cancel().catch(() => undefined);
    reader.releaseLock();
  }
}

export async function* readSseData(
  response: Response, signal: AbortSignal, provider: string,
): AsyncGenerator<string> {
  let data: string[] = [];
  let size = 0;
  for await (const line of readLines(response, signal, provider)) {
    if (line === "") {
      if (data.length) yield data.join("\n");
      data = [];
      size = 0;
    } else if (line.startsWith("data:")) {
      const value = line.slice(5).replace(/^ /, "");
      size += value.length + 1;
      if (size > MAX_FRAME_SIZE) throw new ProviderResponseError(provider);
      data.push(value);
    }
    // SSE comments and event/id/retry fields are not text deltas.
  }
  // An unterminated event is never accepted as a completed stream.
}
