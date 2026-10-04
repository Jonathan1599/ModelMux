import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import test from "node:test";
import { Redis } from "ioredis";
import { chatCacheKey } from "../../src/cache/cache-key";
import { RedisChatCache } from "../../src/cache/redis-chat-cache";
import { loadConfig } from "../../src/config";
import type { ChatRequest, ChatResponse } from "../../src/types/chat";

test("real Redis: exact-cache round trip, byte limits, owner isolation, atomic expiry and first-writer wins", { timeout: 10_000 }, async (t) => {
  let cleanupKey: string | undefined;
  const redis = new Redis(loadConfig().redisUrl, {
    lazyConnect: true, connectTimeout: 2000, commandTimeout: 2000,
    enableOfflineQueue: false, maxRetriesPerRequest: 0, autoResendUnfulfilledCommands: false,
    retryStrategy: () => null,
  });
  redis.on("error", () => {});
  t.after(async () => {
    try {
      if (cleanupKey && redis.status === "ready") await redis.del(cleanupKey);
    } finally { redis.disconnect(); }
  });
  try { await redis.connect(); } catch (error) {
    const code = (error as NodeJS.ErrnoException).code;
    const safeCode = ["ECONNREFUSED", "EPERM", "EACCES", "ETIMEDOUT"].includes(code ?? "") ? code : "unavailable";
    throw new Error(`Cannot connect to configured Redis for the cache integration test (${safeCode})`);
  }
  const namespace = `test-${randomUUID()}`;
  const request: ChatRequest = { model: "test-model", messages: [{ role: "user", content: "test prompt" }] };
  const answer: ChatResponse = { message: { role: "assistant", content: "test answer 🌍" } };
  const signal = new AbortController().signal;
  const key = chatCacheKey(namespace, "test-owner", request);
  // Only this test's exact, random-namespaced key is removed. Never FLUSHDB.
  cleanupKey = key;
  const cache = new RedisChatCache(redis, { namespace, ttlSeconds: 1, timeoutMs: 1000, maxEntryBytes: 128 });
  assert.equal(await cache.get("test-owner", request, signal), null);
  await cache.put("test-owner", request, answer, signal);
  assert.deepEqual(await cache.get("test-owner", request, signal), answer);
  assert.equal(await cache.get("other-owner", request, signal), null);
  assert.ok(await redis.pttl(key) > 0);
  assert.ok(await redis.pttl(key) <= 1000);
  await cache.put("test-owner", request, { message: { role: "assistant", content: "racing answer" } }, signal);
  assert.deepEqual(await cache.get("test-owner", request, signal), answer);
  await delay(1100);
  assert.equal(await cache.get("test-owner", request, signal), null);
  assert.equal(await redis.exists(key), 0);

  await cache.put("test-owner", request, { message: { role: "assistant", content: "🌍".repeat(50) } }, signal);
  assert.equal(await redis.exists(key), 0);
  await redis.set(key, "x".repeat(1024), "EX", 10);
  assert.equal(await cache.get("test-owner", request, signal), null);
  await redis.set(key, "not-json", "EX", 10);
  assert.equal(await cache.get("test-owner", request, signal), null);
});
