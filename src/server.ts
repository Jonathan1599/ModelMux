import { Redis } from "ioredis";
import pg from "pg";
import { buildApp } from "./app";
import { PostgresApiKeyStore } from "./auth/postgres-api-key-store";
import { StoredApiKeyAuthenticator } from "./auth/api-keys";
import { RedisChatCache } from "./cache/redis-chat-cache";
import { chatCacheNamespace } from "./cache/cache-key";
import { loadConfig, providerExecutionTimeoutMs } from "./config";
import { InMemoryConcurrencyLimiter } from "./concurrency/in-memory-concurrency-limiter";
import { createProvider } from "./providers/create-provider";
import { RedisTokenBucket } from "./rate-limit/redis-token-bucket";
import { RabbitMqJobQueue } from "./queue/rabbitmq";
import { PostgresJobStore } from "./queue/postgres-job-store";
import type { ChatRequest, ChatResponse } from "./types/chat";
import { INFERENCE_QUEUE_NAME } from "./jobs/inference-jobs";

const { Pool } = pg;

const config = loadConfig();
const pool = new Pool({
  connectionString: config.databaseUrl,
  connectionTimeoutMillis: 5_000,
  statement_timeout: 10_000,
});
const redis = new Redis(config.redisUrl, {
  lazyConnect: true,
  maxRetriesPerRequest: 1,
});
// Same Redis server, separate connection: cache failures must not queue commands
// indefinitely or change the rate limiter's connection policy.
const cacheRedis = config.chatCache.ttlSeconds > 0 ? new Redis(config.redisUrl, {
  lazyConnect: true,
  enableOfflineQueue: false,
  maxRetriesPerRequest: 0,
  autoResendUnfulfilledCommands: false,
  commandTimeout: config.chatCache.timeoutMs,
}) : undefined;
const chatCache = cacheRedis ? new RedisChatCache(cacheRedis, {
  ...config.chatCache,
  namespace: chatCacheNamespace(config),
}) : undefined;
const apiKeyStore = new PostgresApiKeyStore(pool);
const jobStore = new PostgresJobStore<ChatRequest, ChatResponse>(pool, {
  name: INFERENCE_QUEUE_NAME,
  maxAttempts: config.jobMaxAttempts,
  backoffMs: config.jobBackoffMs,
  leaseMs: providerExecutionTimeoutMs(config) + 30_000,
});
const apiKeyAuthenticator = new StoredApiKeyAuthenticator(apiKeyStore);
const rateLimiter = new RedisTokenBucket(redis);
const concurrencyLimiter = new InMemoryConcurrencyLimiter({
  maxConcurrent: config.providerMaxConcurrency,
  maxQueueSize: config.providerMaxQueueSize,
  waitTimeoutMs: config.providerQueueTimeoutMs,
});
const provider = createProvider(config);
const app = buildApp({
  provider,
  providerHealth: () => provider.snapshot(),
  apiKeyAuthenticator,
  rateLimiter,
  concurrencyLimiter,
  chatCache,
  jobs: new RabbitMqJobQueue(jobStore),
});
let isShuttingDown = false;

pool.on("error", (error) => {
  app.log.error({ err: error }, "Idle Postgres client failed");
});

redis.on("error", (error) => {
  app.log.error({ err: error }, "Redis connection failed");
});
cacheRedis?.on("error", () => {
  app.log.warn("Chat cache Redis connection unavailable");
});

async function start(): Promise<void> {
  try {
    // Optional optimization: a cache connection must not gate server startup.
    void cacheRedis?.connect().catch(() => app.log.warn("Chat cache initially unavailable; requests will bypass it"));
    await Promise.all([pool.query("SELECT 1"), redis.connect()]);
    await app.listen({ port: config.port, host: "0.0.0.0" });
  } catch (error) {
    app.log.fatal({ err: error }, "Failed to start gateway");
    redis.disconnect();
    cacheRedis?.disconnect();
    await pool.end().catch(() => undefined);
    process.exitCode = 1;
  }
}

async function shutdown(signal: NodeJS.Signals): Promise<void> {
  if (isShuttingDown) {
    return;
  }

  isShuttingDown = true;
  app.log.info({ signal }, "Shutting down gateway");

  try {
    await app.close();
  } catch (error) {
    app.log.error({ err: error }, "Failed to close the HTTP server cleanly");
    process.exitCode = 1;
  }

  redis.disconnect();
  cacheRedis?.disconnect();

  try {
    await pool.end();
  } catch (error) {
    app.log.error({ err: error }, "Failed to close the Postgres pool cleanly");
    process.exitCode = 1;
  }
}

process.once("SIGINT", () => void shutdown("SIGINT"));
process.once("SIGTERM", () => void shutdown("SIGTERM"));

void start();
