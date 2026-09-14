import assert from "node:assert/strict";
import test from "node:test";
import type { Redis } from "ioredis";
import { RateLimiterUnavailableError } from "../src/errors.js";
import { RedisTokenBucket } from "../src/rate-limit/redis-token-bucket.js";

test("Redis token bucket scopes state by API key and returns its decision", async () => {
  let receivedArguments: unknown[] | undefined;
  const redis = {
    async eval(...args: unknown[]) {
      receivedArguments = args;
      return [0, 0, 4];
    },
  } as unknown as Redis;
  const limiter = new RedisTokenBucket(redis);

  const decision = await limiter.consume("key-123", {
    capacity: 20,
    refillPerSecond: 2.5,
  });

  assert.deepEqual(decision, {
    allowed: false,
    remaining: 0,
    retryAfterSeconds: 4,
  });
  assert.equal(receivedArguments?.[1], 1);
  assert.equal(receivedArguments?.[2], "modelmux:rate-limit:{key-123}");
  assert.equal(receivedArguments?.[3], 20);
  assert.equal(receivedArguments?.[4], 2.5);
});

test("Redis token bucket fails closed when Redis fails", async () => {
  const redis = {
    async eval() {
      throw new Error("Redis is unavailable");
    },
  } as unknown as Redis;
  const limiter = new RedisTokenBucket(redis);

  await assert.rejects(
    limiter.consume("key-123", { capacity: 10, refillPerSecond: 1 }),
    RateLimiterUnavailableError,
  );
});
