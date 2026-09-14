import type { Redis } from "ioredis";
import { RateLimiterUnavailableError } from "../errors.js";
import type {
  RateLimitDecision,
  RateLimiter,
  RateLimitPolicy,
} from "./rate-limiter.js";

const TOKEN_BUCKET_SCRIPT = `
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_per_second = tonumber(ARGV[2])
local cost = 1

local redis_time = redis.call("TIME")
local now_ms = tonumber(redis_time[1]) * 1000 + math.floor(tonumber(redis_time[2]) / 1000)
local state = redis.call("HMGET", key, "tokens", "updated_at_ms")
local tokens = tonumber(state[1])
local updated_at_ms = tonumber(state[2])

if tokens == nil then
  tokens = capacity
end

if updated_at_ms == nil then
  updated_at_ms = now_ms
end

local elapsed_ms = math.max(0, now_ms - updated_at_ms)
tokens = math.min(capacity, tokens + (elapsed_ms / 1000) * refill_per_second)

local allowed = 0
local retry_after_seconds = 0

if tokens >= cost then
  allowed = 1
  tokens = tokens - cost
else
  retry_after_seconds = math.max(1, math.ceil((cost - tokens) / refill_per_second))
end

redis.call("HSET", key, "tokens", tokens, "updated_at_ms", now_ms)
local ttl_seconds = math.max(1, math.ceil((capacity / refill_per_second) * 2))
redis.call("EXPIRE", key, ttl_seconds)

return { allowed, math.floor(tokens), retry_after_seconds }
`;

export class RedisTokenBucket implements RateLimiter {
  public constructor(private readonly redis: Redis) {}

  public async consume(
    subject: string,
    policy: RateLimitPolicy,
  ): Promise<RateLimitDecision> {
    validatePolicy(policy);

    try {
      const result = await this.redis.eval(
        TOKEN_BUCKET_SCRIPT,
        1,
        `modelmux:rate-limit:{${subject}}`,
        policy.capacity,
        policy.refillPerSecond,
      );

      if (!isTokenBucketResult(result)) {
        throw new Error("Redis returned an invalid token bucket result");
      }

      return {
        allowed: result[0] === 1,
        remaining: result[1],
        retryAfterSeconds: result[2],
      };
    } catch (cause) {
      throw new RateLimiterUnavailableError(cause);
    }
  }
}

function validatePolicy(policy: RateLimitPolicy): void {
  if (
    !Number.isInteger(policy.capacity) ||
    policy.capacity < 1 ||
    !Number.isFinite(policy.refillPerSecond) ||
    policy.refillPerSecond <= 0
  ) {
    throw new RateLimiterUnavailableError(
      new Error("API key has an invalid rate limit policy"),
    );
  }
}

function isTokenBucketResult(value: unknown): value is [number, number, number] {
  return (
    Array.isArray(value) &&
    value.length === 3 &&
    value.every((item) => typeof item === "number" && Number.isFinite(item))
  );
}
