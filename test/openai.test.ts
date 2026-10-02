import assert from "node:assert/strict";
import test from "node:test";
import {
  ProviderConnectionError,
  ProviderHttpError,
  ProviderResponseError,
  ProviderTimeoutError,
} from "../src/errors";
import { OpenAIProvider } from "../src/providers/openai";
import type { ChatRequest } from "../src/types/chat";

const request: ChatRequest = {
  model: "hosted-test-model",
  messages: [
    { role: "system", content: "Answer briefly." },
    { role: "user", content: "Hello" },
    { role: "assistant", content: "Hi" },
    { role: "user", content: "Explain TCP." },
  ],
};

test("OpenAIProvider translates chat requests and responses without retaining upstream metadata", async (t) => {
  t.mock.method(globalThis, "fetch", async (input: string | URL | Request, init?: RequestInit) => {
    assert.equal(input.toString(), "https://api.openai.com/v1/chat/completions");
    assert.equal(init?.method, "POST");
    assert.equal(init?.redirect, "error");
    assert.deepEqual(init?.headers, {
      authorization: "Bearer test-api-key",
      "content-type": "application/json",
    });
    assert.ok(init?.signal instanceof AbortSignal);
    assert.deepEqual(JSON.parse(String(init?.body)), {
      model: request.model,
      messages: request.messages,
      stream: false,
      store: false,
    });

    return Response.json({
      id: "provider-request-id",
      choices: [{ message: { role: "assistant", content: "TCP provides reliable delivery." } }],
      usage: { total_tokens: 20 },
    });
  });

  const provider = new OpenAIProvider("test-api-key");
  assert.deepEqual(await provider.chat(request), {
    message: { role: "assistant", content: "TCP provides reliable delivery." },
  });
});

test("OpenAIProvider accepts empty text and returns refusals as completed assistant messages", async (t) => {
  for (const [message, content] of [
    [{ role: "assistant", content: "" }, ""],
    [{ role: "assistant", content: null, refusal: "I cannot help with that." }, "I cannot help with that."],
    [{ role: "assistant", refusal: "I cannot help with that." }, "I cannot help with that."],
  ] as const) {
    const fetchMock = t.mock.method(globalThis, "fetch", async () =>
      Response.json({ choices: [{ message }] }),
    );

    assert.deepEqual(await new OpenAIProvider("test-api-key").chat(request), {
      message: { role: "assistant", content },
    });
    fetchMock.mock.restore();
  }
});

test("OpenAIProvider preserves HTTP status without retaining sensitive error bodies or retrying", async (t) => {
  for (const status of [400, 401, 403, 404, 429, 500, 503]) {
    let cancelled = false;
    const fetchMock = t.mock.method(globalThis, "fetch", async () => new Response(
      new ReadableStream({
        start(controller) {
          controller.enqueue(new TextEncoder().encode("secret-api-key and private prompt"));
        },
        cancel() {
          cancelled = true;
        },
      }),
      { status },
    ));

    await assert.rejects(new OpenAIProvider("test-api-key").chat(request), (error: unknown) => {
      assert.ok(error instanceof ProviderHttpError);
      assert.equal(error.provider, "OpenAI");
      assert.equal(error.upstreamStatus, status);
      assert.equal(error.upstreamDetail, undefined);
      assert.equal(error.cause, undefined);
      assert.equal(error.message, "OpenAI request failed");
      return true;
    });
    assert.equal(cancelled, true);
    assert.equal(fetchMock.mock.callCount(), 1);
    fetchMock.mock.restore();
  }
});

test("OpenAIProvider classifies network failures without copying sensitive causes", async (t) => {
  const fetchMock = t.mock.method(globalThis, "fetch", async () => {
    throw new TypeError("network failed: secret-api-key");
  });

  await assert.rejects(new OpenAIProvider("test-api-key").chat(request), (error: unknown) => {
    assert.ok(error instanceof ProviderConnectionError);
    assert.equal(error.cause, undefined);
    assert.doesNotMatch(error.message, /secret-api-key/);
    return true;
  });
  assert.equal(fetchMock.mock.callCount(), 1);
});

test("OpenAIProvider rejects malformed JSON without retaining the parser's response excerpt", async (t) => {
  t.mock.method(globalThis, "fetch", async () => new Response("secret-api-key: not JSON"));

  await assert.rejects(new OpenAIProvider("test-api-key").chat(request), (error: unknown) => {
    assert.ok(error instanceof ProviderResponseError);
    assert.equal(error.cause, undefined);
    assert.doesNotMatch(error.message, /secret-api-key/);
    return true;
  });
});

test("OpenAIProvider validates nested response shapes and the assistant role", async (t) => {
  const malformed: unknown[] = [
    null,
    [],
    "answer",
    {},
    { choices: null },
    { choices: {} },
    { choices: [] },
    { choices: [null] },
    { choices: [[]] },
    { choices: [{}] },
    ...[null, [], {}, { content: "answer" }, { role: "user", content: "answer" },
      { role: "assistant" }, { role: "assistant", content: null },
      { role: "assistant", content: 1 }, { role: "assistant", content: [] },
      { role: "assistant", content: null, refusal: 1 },
      { role: "assistant", content: [], refusal: "refusal" },
    ].map((message) => ({ choices: [{ message }] })),
  ];

  for (const body of malformed) {
    const fetchMock = t.mock.method(globalThis, "fetch", async () => Response.json(body));
    await assert.rejects(new OpenAIProvider("test-api-key").chat(request), ProviderResponseError);
    fetchMock.mock.restore();
  }
});

test("OpenAIProvider aborts when the response headers exceed its timeout", async (t) => {
  t.mock.method(globalThis, "fetch", async (_input: unknown, init?: RequestInit) =>
    new Promise<Response>((resolve, reject) => {
      // Keep the event loop alive because AbortSignal.timeout uses an unref'ed timer.
      const fallback = setTimeout(() => resolve(Response.json({})), 1_000);
      init?.signal?.addEventListener("abort", () => {
        clearTimeout(fallback);
        reject(init.signal?.reason);
      }, { once: true });
    }),
  );

  await assert.rejects(new OpenAIProvider("test-api-key", 10).chat(request), (error: unknown) => {
    assert.ok(error instanceof ProviderTimeoutError);
    assert.equal(error.timeoutMs, 10);
    return true;
  });
});

test("OpenAIProvider keeps its timeout active while reading the successful response body", async (t) => {
  t.mock.method(globalThis, "fetch", async (_input: unknown, init?: RequestInit) => new Response(
    new ReadableStream({
      start(controller) {
        const fallback = setTimeout(() => controller.close(), 1_000);
        init?.signal?.addEventListener("abort", () => {
          clearTimeout(fallback);
          controller.error(init.signal?.reason);
        }, { once: true });
      },
    }),
  ));

  await assert.rejects(new OpenAIProvider("test-api-key", 10).chat(request), ProviderTimeoutError);
});

test("OpenAIProvider rejects unusable credentials and timeouts at construction", () => {
  for (const apiKey of ["", " ", " key", "key\n", "key token", "key\u0000", "key\u00e9"]) {
    assert.throws(() => new OpenAIProvider(apiKey), /OpenAI API key/);
  }

  for (const timeoutMs of [0, -1, 1.5, NaN, Infinity, 2_147_483_648]) {
    assert.throws(() => new OpenAIProvider("test-api-key", timeoutMs), /OpenAI request timeout/);
  }
});
