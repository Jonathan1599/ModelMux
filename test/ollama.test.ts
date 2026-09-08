import assert from "node:assert/strict";
import test from "node:test";
import {
  ProviderConnectionError,
  ProviderHttpError,
} from "../src/errors.js";
import { OllamaProvider } from "../src/providers/ollama.js";
import type { ChatRequest } from "../src/types/chat.js";

const request: ChatRequest = {
  model: "llama3.2",
  messages: [{ role: "user", content: "Hello" }],
};

test("OllamaProvider sends a non-streaming request and translates the response", async (t) => {
  const originalFetch = globalThis.fetch;
  t.after(() => {
    globalThis.fetch = originalFetch;
  });

  globalThis.fetch = async (input, init) => {
    assert.equal(input.toString(), "http://ollama.test:11434/api/chat");
    assert.equal(init?.method, "POST");
    assert.deepEqual(JSON.parse(String(init?.body)), {
      model: "llama3.2",
      messages: [{ role: "user", content: "Hello" }],
      stream: false,
    });

    return new Response(
      JSON.stringify({
        message: { role: "assistant", content: "Hello from Ollama" },
      }),
      { status: 200, headers: { "content-type": "application/json" } },
    );
  };

  const provider = new OllamaProvider("http://ollama.test:11434");
  const response = await provider.chat(request);

  assert.deepEqual(response, {
    message: { role: "assistant", content: "Hello from Ollama" },
  });
});

test("OllamaProvider turns non-2xx responses into typed provider errors", async (t) => {
  const originalFetch = globalThis.fetch;
  t.after(() => {
    globalThis.fetch = originalFetch;
  });

  globalThis.fetch = async () =>
    new Response(JSON.stringify({ error: "model not found" }), {
      status: 404,
      headers: { "content-type": "application/json" },
    });

  const provider = new OllamaProvider("http://ollama.test:11434");

  await assert.rejects(provider.chat(request), (error: unknown) => {
    assert.ok(error instanceof ProviderHttpError);
    assert.equal(error.upstreamStatus, 404);
    assert.equal(error.statusCode, 502);
    assert.match(error.message, /model not found/);
    return true;
  });
});

test("OllamaProvider turns fetch failures into a typed unavailable error", async (t) => {
  const originalFetch = globalThis.fetch;
  t.after(() => {
    globalThis.fetch = originalFetch;
  });

  globalThis.fetch = async () => {
    throw new TypeError("fetch failed");
  };

  const provider = new OllamaProvider("http://ollama.test:11434");

  await assert.rejects(provider.chat(request), ProviderConnectionError);
});
