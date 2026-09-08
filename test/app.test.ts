import assert from "node:assert/strict";
import test from "node:test";
import { buildApp } from "../src/app.js";
import { ProviderConnectionError } from "../src/errors.js";
import type { LLMProvider } from "../src/providers/provider.js";
import type { ChatRequest, ChatResponse } from "../src/types/chat.js";

class StubProvider implements LLMProvider {
  public constructor(
    private readonly handler: (request: ChatRequest) => Promise<ChatResponse>,
  ) {}

  public chat(request: ChatRequest): Promise<ChatResponse> {
    return this.handler(request);
  }
}

test("GET /health reports that the process is up", async (t) => {
  const provider = new StubProvider(async () => {
    throw new Error("Provider should not be called");
  });
  const app = buildApp({ provider, logger: false });
  t.after(() => app.close());

  const response = await app.inject({ method: "GET", url: "/health" });

  assert.equal(response.statusCode, 200);
  assert.deepEqual(response.json(), { status: "ok" });
  assert.ok(response.headers["x-request-id"]);
});

test("POST /v1/chat forwards a valid provider-agnostic request", async (t) => {
  let receivedRequest: ChatRequest | undefined;
  const provider = new StubProvider(async (request) => {
    receivedRequest = request;
    return {
      message: {
        role: "assistant",
        content: "TCP reliably delivers an ordered stream of bytes between hosts.",
      },
    };
  });
  const app = buildApp({ provider, logger: false });
  t.after(() => app.close());

  const payload: ChatRequest = {
    model: "llama3.2",
    messages: [{ role: "user", content: "Explain TCP in one sentence." }],
  };
  const response = await app.inject({
    method: "POST",
    url: "/v1/chat",
    payload,
  });

  assert.equal(response.statusCode, 200);
  assert.deepEqual(receivedRequest, payload);
  assert.deepEqual(response.json(), {
    message: {
      role: "assistant",
      content: "TCP reliably delivers an ordered stream of bytes between hosts.",
    },
  });
});

test("POST /v1/chat rejects malformed request bodies", async (t) => {
  let callCount = 0;
  const provider = new StubProvider(async () => {
    callCount += 1;
    return { message: { role: "assistant", content: "unexpected" } };
  });
  const app = buildApp({ provider, logger: false });
  t.after(() => app.close());

  const invalidPayloads = [
    { model: "llama3.2" },
    { model: "llama3.2", messages: [] },
    {
      model: "llama3.2",
      messages: [{ role: "invalid", content: "hello" }],
    },
  ];

  for (const payload of invalidPayloads) {
    const response = await app.inject({
      method: "POST",
      url: "/v1/chat",
      payload,
    });

    assert.equal(response.statusCode, 400);
    assert.equal(response.json().error.code, "VALIDATION_ERROR");
  }

  assert.equal(callCount, 0);
});

test("POST /v1/chat returns 400 for invalid JSON", async (t) => {
  const provider = new StubProvider(async () => {
    throw new Error("Provider should not be called");
  });
  const app = buildApp({ provider, logger: false });
  t.after(() => app.close());

  const response = await app.inject({
    method: "POST",
    url: "/v1/chat",
    headers: { "content-type": "application/json" },
    payload: '{"model":',
  });

  assert.equal(response.statusCode, 400);
  assert.equal(response.json().error.code, "FST_ERR_CTP_INVALID_JSON_BODY");
});

test("POST /v1/chat maps provider connection failures to 503", async (t) => {
  const provider = new StubProvider(async () => {
    throw new ProviderConnectionError("Ollama");
  });
  const app = buildApp({ provider, logger: false });
  t.after(() => app.close());

  const response = await app.inject({
    method: "POST",
    url: "/v1/chat",
    payload: {
      model: "llama3.2",
      messages: [{ role: "user", content: "Hello" }],
    },
  });

  assert.equal(response.statusCode, 503);
  assert.equal(response.json().error.code, "PROVIDER_UNAVAILABLE");
  assert.equal(response.json().error.message, "Unable to connect to Ollama");
});
