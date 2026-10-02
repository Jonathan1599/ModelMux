import assert from "node:assert/strict";
import test from "node:test";
import pino from "pino";
import { loadConfig } from "../src/config";
import { ProviderCircuitOpenError, ProviderHttpError } from "../src/errors";
import { createProvider } from "../src/providers/create-provider";
import type { ChatRequest } from "../src/types/chat";

const request: ChatRequest = {
  model: "llama3.2",
  messages: [{ role: "user", content: "Explain TCP." }],
};
const fallbackEnv = {
  OLLAMA_BASE_URL: "http://ollama.test:11434",
  OPENAI_API_KEY: "test-api-key",
  OPENAI_FALLBACK_MODEL_MAP: '{"llama3.2":"hosted-test-model"}',
};

test("createProvider uses the configured Ollama endpoint first and accepts calls without a logger", async (t) => {
  const fetchMock = t.mock.method(globalThis, "fetch", async (input: string | URL | Request, init?: RequestInit) => {
    assert.equal(input.toString(), "http://ollama.test:11434/api/chat");
    assert.deepEqual(JSON.parse(String(init?.body)), { ...request, stream: false });
    return Response.json({ message: { role: "assistant", content: "Local answer" } });
  });

  const provider = createProvider(loadConfig(fallbackEnv));
  assert.deepEqual(await provider.chat(request), {
    message: { role: "assistant", content: "Local answer" },
  });
  assert.equal(fetchMock.mock.callCount(), 1);
  assert.deepEqual(provider.snapshot().map(({ name, state }) => ({ name, state })), [
    { name: "ollama", state: "closed" },
    { name: "openai", state: "closed" },
  ]);
});

test("createProvider maps the model on transient primary failure and logs the fallback through context", async (t) => {
  const calls: string[] = [];
  const logLines: string[] = [];
  const logger = pino({}, { write(line) { logLines.push(line); } });
  t.mock.method(globalThis, "fetch", async (input: string | URL | Request, init?: RequestInit) => {
    const url = input.toString();
    calls.push(url);

    if (url === "http://ollama.test:11434/api/chat") {
      return Response.json({ error: "provider busy" }, { status: 503 });
    }

    assert.equal(url, "https://api.openai.com/v1/chat/completions");
    assert.deepEqual(JSON.parse(String(init?.body)), {
      model: "hosted-test-model", messages: request.messages, stream: false, store: false,
    });
    assert.equal(new Headers(init?.headers).get("authorization"), "Bearer test-api-key");
    return Response.json({ choices: [{ message: { role: "assistant", content: "Hosted answer" } }] });
  });

  const provider = createProvider(loadConfig(fallbackEnv));
  assert.deepEqual(await provider.chat(request, { logger }), {
    message: { role: "assistant", content: "Hosted answer" },
  });
  assert.deepEqual(calls, [
    "http://ollama.test:11434/api/chat", "https://api.openai.com/v1/chat/completions",
  ]);
  const logs = logLines.map((line) => JSON.parse(line) as Record<string, unknown>);
  assert.ok(logs.some((log) => log.primary === "ollama" && log.fallback === "openai"));
  assert.ok(logs.some((log) => log.provider === "openai" && typeof log.providerLatencyMs === "number"));
  assert.doesNotMatch(logLines.join(""), /test-api-key|Explain TCP/);
  assert.equal(request.model, "llama3.2");
});

test("createProvider never sends unmapped requests to OpenAI, including when a key alone is configured", async (t) => {
  const fetchMock = t.mock.method(globalThis, "fetch", async (input: string | URL | Request) => {
    assert.equal(input.toString(), "http://localhost:11434/api/chat");
    return Response.json({ error: "provider busy" }, { status: 503 });
  });
  const environments: NodeJS.ProcessEnv[] = [
    {},
    { OPENAI_API_KEY: "test-api-key" },
    { OPENAI_FALLBACK_MODEL_MAP: "{}" },
    { OPENAI_API_KEY: "test-api-key", OPENAI_FALLBACK_MODEL_MAP: '{"other-model":"hosted-test-model"}' },
  ];

  for (const env of environments) {
    const provider = createProvider(loadConfig(env));
    await assert.rejects(provider.chat(request), ProviderHttpError);
  }
  assert.equal(fetchMock.mock.callCount(), environments.length);
});

test("createProvider creates independent circuits for gateway and worker instances", async (t) => {
  let calls = 0;
  t.mock.method(globalThis, "fetch", async () => {
    calls += 1;
    return calls === 1
      ? Response.json({ error: "provider busy" }, { status: 503 })
      : Response.json({ message: { role: "assistant", content: "Recovered" } });
  });
  const config = loadConfig({ CIRCUIT_BREAKER_FAILURE_THRESHOLD: "1" });
  const gatewayProvider = createProvider(config);
  const workerProvider = createProvider(config);

  await assert.rejects(gatewayProvider.chat(request), ProviderHttpError);
  assert.equal(gatewayProvider.snapshot()[0]?.state, "open");
  assert.equal(workerProvider.snapshot()[0]?.state, "closed");
  assert.deepEqual(await workerProvider.chat(request), {
    message: { role: "assistant", content: "Recovered" },
  });
  await assert.rejects(gatewayProvider.chat(request), ProviderCircuitOpenError);
  assert.equal(calls, 2);
});
