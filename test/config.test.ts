import assert from "node:assert/strict";
import test from "node:test";
import { loadConfig, providerExecutionTimeoutMs } from "../src/config";

test("exact cache settings have safe defaults, accept overrides, and allow disabling", () => {
  assert.deepEqual(loadConfig({}).chatCache, { ttlSeconds: 300, maxEntryBytes: 65536, timeoutMs: 100, version: "1" });
  assert.deepEqual(loadConfig({
    CHAT_CACHE_TTL_SECONDS: "0", CHAT_CACHE_MAX_ENTRY_BYTES: "1024",
    CHAT_CACHE_TIMEOUT_MS: "50", CHAT_CACHE_VERSION: " model-revision-2 ",
  }).chatCache, { ttlSeconds: 0, maxEntryBytes: 1024, timeoutMs: 50, version: "model-revision-2" });
  for (const name of ["CHAT_CACHE_TTL_SECONDS", "CHAT_CACHE_MAX_ENTRY_BYTES", "CHAT_CACHE_TIMEOUT_MS"]) {
    for (const value of ["-1", "1.5", "bad", "Infinity", "2147483648"]) {
      assert.throws(() => loadConfig({ [name]: value }), new RegExp(name));
    }
  }
  assert.throws(() => loadConfig({ CHAT_CACHE_MAX_ENTRY_BYTES: "0" }), /CHAT_CACHE_MAX_ENTRY_BYTES/);
  assert.throws(() => loadConfig({ CHAT_CACHE_TIMEOUT_MS: "0" }), /CHAT_CACHE_TIMEOUT_MS/);
});

test("async job configuration has defaults and accepts overrides", () => {
  const defaults = loadConfig({});
  assert.equal(defaults.jobConcurrency, 2);
  assert.equal(defaults.jobMaxAttempts, 3);
  assert.equal(defaults.jobBackoffMs, 1_000);
  assert.equal(defaults.rabbitmqUrl, "amqp://modelmux:modelmux@localhost:5672");
  const config = loadConfig({ JOB_CONCURRENCY: "1", JOB_MAX_ATTEMPTS: "5", JOB_BACKOFF_MS: "250" });
  assert.equal(config.jobConcurrency, 1);
  assert.equal(config.jobMaxAttempts, 5);
  assert.equal(config.jobBackoffMs, 250);
});

test("RabbitMQ configuration accepts AMQP/TLS URLs and rejects other protocols", () => {
  assert.equal(loadConfig({ RABBITMQ_URL: "amqps://localhost:5671" }).rabbitmqUrl, "amqps://localhost:5671");
  assert.throws(() => loadConfig({ RABBITMQ_URL: "http://localhost" }), /RABBITMQ_URL/);
});

test("async job configuration rejects invalid concurrency, attempts, and backoff", () => {
  for (const name of ["JOB_CONCURRENCY", "JOB_MAX_ATTEMPTS", "JOB_BACKOFF_MS"]) {
    for (const value of ["0", "-1", "1.5", "bad", "Infinity", "2147483648"]) {
      assert.throws(() => loadConfig({ [name]: value }), new RegExp(name));
    }
  }
});

test("hosted fallback requires an explicit model map; a key alone keeps it disabled", () => {
  for (const env of [{}, { OPENAI_API_KEY: "test-only-key" }, { OPENAI_FALLBACK_MODEL_MAP: "{}" }]) {
    const config = loadConfig(env);
    assert.equal(config.openai, undefined);
    assert.equal(providerExecutionTimeoutMs(config), 120_000);
  }
  assert.throws(() => loadConfig({ OPENAI_FALLBACK_MODEL_MAP: '{"llama3.2":"hosted-model"}' }), /OPENAI_API_KEY/);
});

test("fallback model mapping and both timeouts determine the execution budget", () => {
  const config = loadConfig({
    OPENAI_API_KEY: " test-only-key ",
    OPENAI_FALLBACK_MODEL_MAP: '{"llama3.2":"hosted-model","local-other":"hosted-other"}',
    OLLAMA_REQUEST_TIMEOUT_MS: "2000",
    OPENAI_REQUEST_TIMEOUT_MS: "500",
    CIRCUIT_BREAKER_FAILURE_THRESHOLD: "2",
    CIRCUIT_BREAKER_RESET_TIMEOUT_MS: "1000",
  });
  assert.deepEqual(config.openai, {
    apiKey: "test-only-key", requestTimeoutMs: 500,
    modelMap: { "llama3.2": "hosted-model", "local-other": "hosted-other" },
  });
  assert.equal(providerExecutionTimeoutMs(config), 2500);
  assert.deepEqual(config.circuitBreaker, { failureThreshold: 2, resetTimeoutMs: 1000 });
  const defaults = loadConfig({
    OPENAI_API_KEY: "test-only-key", OPENAI_FALLBACK_MODEL_MAP: '{"llama3.2":"hosted-model"}',
  });
  assert.equal(defaults.openai?.requestTimeoutMs, 30_000);
  assert.deepEqual(defaults.circuitBreaker, { failureThreshold: 5, resetTimeoutMs: 30_000 });
});

test("malformed fallback maps fail startup without echoing their contents", () => {
  for (const value of [
    "secret-invalid-json", "null", "[]", "42", '"model"',
    '{"":"hosted"}', '{"local":null}', '{"local":1}',
    '{"local":{}}', '{"local":[]}', '{"local":""}', '{" local":"hosted"}', '{"local":" hosted "}',
  ]) {
    assert.throws(() => loadConfig({ OPENAI_API_KEY: "test-only-key", OPENAI_FALLBACK_MODEL_MAP: value }),
      (error: unknown) => {
        assert.ok(error instanceof Error);
        assert.match(error.message, /OPENAI_FALLBACK_MODEL_MAP/);
        assert.doesNotMatch(error.message, /secret-invalid-json|test-only-key/);
        return true;
      });
  }
});

test("circuit settings and enabled fallback timeouts reject invalid bounds", () => {
  for (const name of ["CIRCUIT_BREAKER_FAILURE_THRESHOLD", "CIRCUIT_BREAKER_RESET_TIMEOUT_MS", "OPENAI_REQUEST_TIMEOUT_MS"]) {
    for (const value of ["", "0", "-1", "0.5", "bad", "Infinity", "2147483648"]) {
      assert.throws(() => loadConfig({
        OPENAI_API_KEY: "test-only-key", OPENAI_FALLBACK_MODEL_MAP: '{"local":"hosted"}', [name]: value,
      }), new RegExp(name));
    }
  }
});
