import assert from "node:assert/strict";
import { getEventListeners } from "node:events";
import test from "node:test";
import type { Redis } from "ioredis";
import { RequestAbortedError } from "../src/cancellation";
import { chatCacheKey, chatCacheNamespace } from "../src/cache/cache-key";
import { RedisChatCache } from "../src/cache/redis-chat-cache";
import { loadConfig } from "../src/config";
import type { ChatRequest, ChatResponse } from "../src/types/chat";

const request: ChatRequest = {
  model: "local",
  messages: [{ role: "system", content: "Answer carefully" }, { role: "user", content: "private prompt" }],
};
const answer: ChatResponse = { message: { role: "assistant", content: "cached answer 🌍" } };
const signal = () => new AbortController().signal;
const options = { namespace: "test", ttlSeconds: 300, maxEntryBytes: 128, timeoutMs: 20 };

// A byte-oriented Redis stub with an injectable clock; no sleeps for expiry tests.
function fixture() {
  let now = 0;
  const entries = new Map<string, { value: Buffer; expiresAt: number }>();
  const reads: string[] = [];
  const writes: unknown[][] = [];
  const redis = {
    async getrangeBuffer(key: string, start: number, end: number) {
      reads.push(key);
      const entry = entries.get(key);
      if (!entry || entry.expiresAt <= now) return Buffer.alloc(0);
      return entry.value.subarray(start, end + 1);
    },
    async set(key: string, value: string, expiration: string, ttl: number, mode: string) {
      writes.push([key, value, expiration, ttl, mode]);
      assert.equal(expiration, "EX");
      assert.equal(mode, "NX");
      const existing = entries.get(key);
      if (existing && existing.expiresAt > now) return null;
      entries.set(key, { value: Buffer.from(value), expiresAt: now + ttl * 1000 });
      return "OK";
    },
  } as unknown as Pick<Redis, "getrangeBuffer" | "set">;
  return {
    redis, entries, reads, writes,
    cache: new RedisChatCache(redis, options),
    advance(ms: number) { now += ms; },
  };
}

test("cache keys hash owner, full ordered conversation, model and namespace without exposing them", () => {
  const key = chatCacheKey("revision", "owner-1", request);
  assert.match(key, /^modelmux:chat-cache:v1:[a-f0-9]{64}$/);
  assert.doesNotMatch(key, /private prompt|owner-1|revision/);
  assert.equal(key, chatCacheKey("revision", "owner-1", {
    messages: request.messages.map(({ role, content }) => ({ content, role })), model: request.model,
  }));
  for (const [owner, body] of [
    ["owner-2", request], ["owner-1", { ...request, model: "other" }],
    ["owner-1", { ...request, messages: [...request.messages].reverse() }],
    ["owner-1", { ...request, messages: request.messages.slice(1) }],
    ["owner-1", { ...request, messages: [{ role: "user", content: "private prompt " }] }],
  ] satisfies Array<[string, ChatRequest]>) assert.notEqual(key, chatCacheKey("revision", owner, body));
  assert.notEqual(key, chatCacheKey("revision-2", "owner-1", request));
});

test("cache namespace changes with routing configuration, credentials and explicit model revision", () => {
  const config = loadConfig({ OPENAI_API_KEY: "secret", OPENAI_FALLBACK_MODEL_MAP: '{"local":"hosted","other":"else"}' });
  const namespace = chatCacheNamespace(config);
  assert.equal(namespace, chatCacheNamespace({ ...config, openai: { ...config.openai!, modelMap: { other: "else", local: "hosted" } } }));
  for (const changed of [
    { ...config, ollamaBaseUrl: "http://another-provider" },
    { ...config, ollamaRequestTimeoutMs: 10 },
    { ...config, chatCache: { ...config.chatCache, version: "2" } },
    { ...config, openai: undefined },
    { ...config, openai: { ...config.openai!, apiKey: "new-secret" } },
    { ...config, openai: { ...config.openai!, modelMap: { local: "new-model" } } },
  ]) assert.notEqual(namespace, chatCacheNamespace(changed));
  assert.doesNotMatch(namespace, /secret|hosted/);
});

test("Redis cache stores complete answers with atomic TTL, owner isolation and no sliding expiration", async () => {
  const { cache, writes, advance } = fixture();
  assert.equal(await cache.get("alice", request, signal()), null);
  await cache.put("alice", request, answer, signal());
  assert.equal(writes.length, 1);
  assert.deepEqual(writes[0]?.slice(2), ["EX", 300, "NX"]);
  assert.deepEqual(await cache.get("alice", request, signal()), answer);
  assert.equal(await cache.get("bob", request, signal()), null);
  advance(299_000);
  assert.deepEqual(await cache.get("alice", request, signal()), answer);
  // A racing completion does not replace the first answer or renew its expiry.
  await cache.put("alice", request, { message: { role: "assistant", content: "second" } }, signal());
  assert.deepEqual(await cache.get("alice", request, signal()), answer);
  advance(1_000);
  assert.equal(await cache.get("alice", request, signal()), null);
});

test("Redis cache bounds UTF-8 bytes and rejects corrupt, oversized, or invalid response values", async () => {
  const { cache, entries, writes } = fixture();
  const key = chatCacheKey(options.namespace, "alice", request);
  for (const value of [
    Buffer.from("{"), Buffer.from("null"), Buffer.from("[]"), Buffer.from('{"message":{"role":"user","content":"bad"}}'),
    Buffer.from('{"message":{"role":"assistant","content":42}}'), Buffer.from("x".repeat(1000)),
    Buffer.concat([Buffer.from('{"message":{"role":"assistant","content":"'), Buffer.from([0xff]), Buffer.from('"}}')]),
  ]) {
    entries.set(key, { value, expiresAt: Infinity });
    assert.equal(await cache.get("alice", request, signal()), null);
  }
  for (const content of ["🌍".repeat(40), "\n".repeat(60)]) {
    await cache.put("alice", request, { message: { role: "assistant", content } }, signal());
  }
  await cache.put("alice", request, { message: { role: "user", content: "invalid" } } as unknown as ChatResponse, signal());
  assert.equal(writes.length, 0);
});

test("cache values retain only the public assistant answer, never request IDs or provider metadata", async () => {
  const { cache, writes } = fixture();
  const withMetadata = { ...answer, requestId: "old-id", provider: "secret", message: { ...answer.message, extra: "private" } };
  await cache.put("alice", request, withMetadata, signal());
  assert.equal(writes[0]?.[1], JSON.stringify(answer));
  assert.deepEqual(await cache.get("alice", request, signal()), answer);
});

test("bounded cache commands propagate errors, time out, cancel promptly, and clean up listeners", async (t) => {
  const { cache, redis } = fixture();
  for (const method of ["getrangeBuffer", "set"] as const) {
    const call = (abortSignal: AbortSignal) => method === "set"
      ? cache.put("alice", request, answer, abortSignal) : cache.get("alice", request, abortSignal);
    const failure = t.mock.method(redis, method, async () => { throw new Error("offline"); });
    await assert.rejects(call(signal()), /offline/);
    failure.mock.restore();
    const blocked = t.mock.method(redis, method, () => new Promise<never>(() => {}));
    const controller = new AbortController();
    const cancelled = assert.rejects(call(controller.signal), RequestAbortedError);
    controller.abort();
    await cancelled;
    assert.equal(getEventListeners(controller.signal, "abort").length, 0);
    await assert.rejects(call(controller.signal), RequestAbortedError);
    assert.equal(blocked.mock.callCount(), 1);
    const timeoutSignal = signal();
    await assert.rejects(call(timeoutSignal), /Chat cache timed out/);
    assert.equal(getEventListeners(timeoutSignal, "abort").length, 0);
    blocked.mock.restore();
  }
});

test("Redis cache validates its options", () => {
  const { redis } = fixture();
  for (const name of ["ttlSeconds", "maxEntryBytes", "timeoutMs"]) {
    for (const value of [0, -1, 1.5, NaN, Infinity, 2_147_483_648]) {
      assert.throws(() => new RedisChatCache(redis, { ...options, [name]: value }));
    }
  }
  assert.throws(() => new RedisChatCache(redis, { ...options, namespace: "" }));
});
