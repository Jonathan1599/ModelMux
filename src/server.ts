import { Redis } from "ioredis";
import pg from "pg";
import { buildApp } from "./app";
import { PostgresApiKeyStore } from "./auth/postgres-api-key-store";
import { StoredApiKeyAuthenticator } from "./auth/api-keys";
import { loadConfig } from "./config";
import { InMemoryConcurrencyLimiter } from "./concurrency/in-memory-concurrency-limiter";
import { OllamaProvider } from "./providers/ollama";
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
const apiKeyStore = new PostgresApiKeyStore(pool);
const jobStore = new PostgresJobStore<ChatRequest, ChatResponse>(pool, {
  name: INFERENCE_QUEUE_NAME,
  maxAttempts: config.jobMaxAttempts,
  backoffMs: config.jobBackoffMs,
  leaseMs: config.ollamaRequestTimeoutMs + 30_000,
});
const apiKeyAuthenticator = new StoredApiKeyAuthenticator(apiKeyStore);
const rateLimiter = new RedisTokenBucket(redis);
const concurrencyLimiter = new InMemoryConcurrencyLimiter({
  maxConcurrent: config.providerMaxConcurrency,
  maxQueueSize: config.providerMaxQueueSize,
  waitTimeoutMs: config.providerQueueTimeoutMs,
});
const provider = new OllamaProvider(
  config.ollamaBaseUrl,
  config.ollamaRequestTimeoutMs,
);
const app = buildApp({
  provider,
  apiKeyAuthenticator,
  rateLimiter,
  concurrencyLimiter,
  jobs: new RabbitMqJobQueue(jobStore),
});
let isShuttingDown = false;

pool.on("error", (error) => {
  app.log.error({ err: error }, "Idle Postgres client failed");
});

redis.on("error", (error) => {
  app.log.error({ err: error }, "Redis connection failed");
});

async function start(): Promise<void> {
  try {
    await Promise.all([pool.query("SELECT 1"), redis.connect()]);
    await app.listen({ port: config.port, host: "0.0.0.0" });
  } catch (error) {
    app.log.fatal({ err: error }, "Failed to start gateway");
    redis.disconnect();
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
