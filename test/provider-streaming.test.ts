import assert from "node:assert/strict";
import test from "node:test";
import { RequestAbortedError } from "../src/cancellation";
import { ProviderConnectionError, ProviderResponseError, ProviderTimeoutError } from "../src/errors";
import { CircuitBreaker } from "../src/providers/circuit-breaker";
import { OllamaProvider } from "../src/providers/ollama";
import { OpenAIProvider } from "../src/providers/openai";
import { ProviderRouter } from "../src/providers/router";
import type { LLMProvider } from "../src/providers/provider";
import type { ChatRequest } from "../src/types/chat";

const request: ChatRequest = { model: "local", messages: [{ role: "user", content: "secret prompt" }] };
const ollama = new OllamaProvider("http://ollama.test");
const openai = new OpenAIProvider("test-key");
const ndjson = (content: string, done = false) => JSON.stringify({ message: { role: "assistant", content }, done }) + "\n";
const event = (delta: object, finish_reason: string | null = null) =>
  `data: ${JSON.stringify({ choices: [{ index: 0, delta, finish_reason }] })}\r\n\r\n`;

function fragmented(text: string): Response {
  const bytes = new TextEncoder().encode(text);
  let offset = 0;
  return new Response(new ReadableStream<Uint8Array>({
    pull(controller) {
      if (offset < bytes.length) controller.enqueue(bytes.slice(offset, ++offset));
      else controller.close();
    },
  }));
}

test("Ollama streams split UTF-8 NDJSON, awaits each consumer, and cancels unread body after done", async (t) => {
  let cancelled = false;
  let unblock!: () => void;
  let first!: () => void;
  const firstSeen = new Promise<void>((resolve) => { first = resolve; });
  const blocked = new Promise<void>((resolve) => { unblock = resolve; });
  t.mock.method(globalThis, "fetch", async (_url: unknown, init?: RequestInit) => {
    assert.equal(JSON.parse(String(init?.body)).stream, true);
    const stream = new ReadableStream({
      start(controller) { controller.enqueue(new TextEncoder().encode(ndjson("first") + ndjson("second", true))); },
      cancel() { cancelled = true; },
    });
    return new Response(stream);
  });
  const received: string[] = [];
  const running = ollama.stream(request, async (content) => {
    received.push(content);
    if (content === "first") { first(); await blocked; }
  });
  await firstSeen;
  assert.deepEqual(received, ["first"]);
  unblock();
  await running;
  assert.deepEqual(received, ["first", "second"]);
  assert.equal(cancelled, true);

  t.mock.method(globalThis, "fetch", async () => fragmented(ndjson("Hello 🌍") + ndjson("!", true)));
  const unicode: string[] = [];
  await ollama.stream(request, async (text) => { unicode.push(text); });
  assert.deepEqual(unicode, ["Hello 🌍", "!"]);
});

test("OpenAI streams SSE comments, role-only deltas, refusal text, split UTF-8 and terminal events", async (t) => {
  t.mock.method(globalThis, "fetch", async (_url: unknown, init?: RequestInit) => {
    assert.equal(JSON.parse(String(init?.body)).stream, true);
    assert.equal(JSON.parse(String(init?.body)).store, false);
    return fragmented(": keep-alive\r\n\r\n" + event({ role: "assistant", content: "" })
      + event({ content: "Hello 🌍" }) + event({ refusal: "Sorry" })
      + event({}, "stop") + 'data: {"choices":[],"usage":{}}\r\n\r\ndata: [DONE]\r\n\r\n');
  });
  const received: string[] = [];
  await openai.stream(request, async (content) => { received.push(content); });
  assert.deepEqual(received, ["Hello 🌍", "Sorry"]);
});

test("providers reject truncated, malformed, and oversized streaming frames", async (t) => {
  for (const [provider, body] of [
    [ollama, ndjson("partial")], [ollama, "null\n"], [ollama, '{"error":"private detail"}\n'],
    [ollama, "x".repeat(65_537)], [ollama, '{"done":false}\n'],
    [openai, event({ content: "partial" })], [openai, "data: [DONE]\n\n"],
    [openai, event({}, "stop") + "data: [DONE]"],
    [openai, "data: null\n\n"], [openai, event({ content: 42 })],
    [openai, "data: " + "x".repeat(65_537)],
  ] as const) {
    const mock = t.mock.method(globalThis, "fetch", async () => new Response(body));
    await assert.rejects(provider.stream(request, async () => {}), ProviderResponseError);
    mock.mock.restore();
  }
});

test("both adapters abort active streamed and non-streamed fetches and skip pre-aborted calls", async (t) => {
  for (const provider of [ollama, openai]) {
    for (const mode of ["chat", "stream"] as const) {
      let started!: () => void;
      const seen = new Promise<void>((resolve) => { started = resolve; });
      let upstreamSignal: AbortSignal | undefined;
      const mock = t.mock.method(globalThis, "fetch", async (_url: unknown, init?: RequestInit) => {
        upstreamSignal = init?.signal ?? undefined;
        started();
        return new Promise<Response>((_resolve, reject) => {
          upstreamSignal!.addEventListener("abort", () => reject(upstreamSignal!.reason), { once: true });
        });
      });
      const controller = new AbortController();
      const call = () => mode === "chat" ? provider.chat(request, { signal: controller.signal })
        : provider.stream(request, async () => {}, { signal: controller.signal });
      const rejected = assert.rejects(call(), RequestAbortedError);
      await seen;
      controller.abort();
      await rejected;
      assert.equal(upstreamSignal?.aborted, true);
      await assert.rejects(call(), RequestAbortedError);
      assert.equal(mock.mock.callCount(), 1);
      mock.mock.restore();
    }
  }
});

test("stream timeout includes waiting for body data and cleans up the upstream reader", async (t) => {
  let cancelled = false;
  t.mock.method(globalThis, "fetch", async () => new Response(new ReadableStream({
    cancel() { cancelled = true; },
  })));
  // Provider timers are unref'ed; keep the test alive while exercising one.
  const keepAlive = setTimeout(() => {}, 1000);
  t.after(() => clearTimeout(keepAlive));
  await assert.rejects(new OllamaProvider("http://ollama.test", 10).stream(request, async () => {}), ProviderTimeoutError);
  await new Promise<void>((resolve) => setImmediate(resolve));
  assert.equal(cancelled, true);
});

test("cancellation after a delta stops both adapters and cancels their body readers", async (t) => {
  for (const [provider, first] of [[ollama, ndjson("partial")], [openai, event({ content: "partial" })]] as const) {
    let cancelled = false;
    const mock = t.mock.method(globalThis, "fetch", async () => new Response(new ReadableStream({
      start(controller) { controller.enqueue(new TextEncoder().encode(first)); },
      cancel() { cancelled = true; },
    })));
    const controller = new AbortController();
    const chunks: string[] = [];
    await assert.rejects(provider.stream(request, async (content) => {
      chunks.push(content);
      controller.abort();
    }, { signal: controller.signal }), RequestAbortedError);
    await new Promise<void>((resolve) => setImmediate(resolve));
    assert.deepEqual(chunks, ["partial"]);
    assert.equal(cancelled, true);
    mock.mock.restore();
  }
});

test("a provider deadline also interrupts a blocked consumer without delivering later deltas", async (t) => {
  let unblock!: () => void;
  const blocked = new Promise<void>((resolve) => { unblock = resolve; });
  t.after(unblock);
  let cancelled = false;
  t.mock.method(globalThis, "fetch", async () => new Response(new ReadableStream({
    start(controller) { controller.enqueue(new TextEncoder().encode(ndjson("first") + ndjson("later", true))); },
    cancel() { cancelled = true; },
  })));
  const chunks: string[] = [];
  const keepAlive = setTimeout(() => {}, 1000);
  t.after(() => clearTimeout(keepAlive));
  await assert.rejects(new OllamaProvider("http://ollama.test", 10).stream(request, async (content) => {
    chunks.push(content);
    await blocked;
  }), ProviderTimeoutError);
  unblock();
  await new Promise<void>((resolve) => setImmediate(resolve));
  assert.deepEqual(chunks, ["first"]);
  assert.equal(cancelled, true);
});

function circuit(now: () => number = () => 0) {
  return new CircuitBreaker({ failureThreshold: 1, resetTimeoutMs: 100, now });
}

test("stream router falls back only before text, maps the model, and counts mid-stream failures", async () => {
  for (const partial of [false, true]) {
    let fallbacks = 0;
    const primary: LLMProvider = {
      async chat() { throw new Error("unused"); },
      async stream(_request, emit) {
        if (partial) await emit("partial answer");
        throw new ProviderConnectionError("primary");
      },
    };
    const primaryCircuit = circuit();
    const router = new ProviderRouter({ name: "primary", provider: primary, circuit: primaryCircuit }, {
      name: "fallback", modelMap: { local: "hosted" }, circuit: circuit(),
      provider: { async chat() { throw new Error("unused"); }, async stream(data, emit) {
        assert.equal(data.model, "hosted");
        fallbacks += 1;
        await emit("fallback answer");
      } },
    });
    const chunks: string[] = [];
    const promise = router.stream(request, async (text) => { chunks.push(text); });
    if (partial) await assert.rejects(promise, ProviderConnectionError);
    else await promise;
    assert.equal(fallbacks, partial ? 0 : 1);
    assert.deepEqual(chunks, [partial ? "partial answer" : "fallback answer"]);
    assert.equal(primaryCircuit.snapshot().state, "open");
  }
});

test("cancelled recovery probes neither close circuits nor count failures or trigger fallback", async () => {
  let now = 0;
  const breaker = circuit(() => now);
  await assert.rejects(breaker.execute(async () => { throw new Error("outage"); }, () => true));
  now = 100;
  const controller = new AbortController();
  let fallbacks = 0;
  const router = new ProviderRouter({ name: "primary", circuit: breaker, provider: {
    async chat() { controller.abort(); throw new ProviderTimeoutError("primary", 1); },
  } }, { name: "fallback", circuit: circuit(), modelMap: { local: "hosted" }, provider: {
    async chat() { fallbacks += 1; throw new Error("must not happen"); },
  } });
  await assert.rejects(router.chat(request, { signal: controller.signal }), RequestAbortedError);
  assert.equal(fallbacks, 0);
  assert.deepEqual(breaker.snapshot(), { state: "open", consecutiveFailures: 1, retryAfterMs: 0 });
  assert.equal(await breaker.execute(async () => "recovered", () => true), "recovered");
});

test("downstream callback errors do not count as provider health outcomes", async () => {
  const breaker = new CircuitBreaker({ failureThreshold: 5, resetTimeoutMs: 100 });
  await assert.rejects(breaker.execute(async () => { throw new Error("previous outage"); }, () => true));
  const router = new ProviderRouter({ name: "primary", circuit: breaker, provider: {
    async chat() { throw new Error("unused"); },
    async stream(_request, emit) { await emit("text"); },
  } });
  await assert.rejects(router.stream(request, async () => { throw new Error("broken consumer"); }));
  assert.equal(breaker.snapshot().consecutiveFailures, 1);
  assert.equal(breaker.snapshot().state, "closed");
});
