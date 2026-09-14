import { Redis } from "ioredis";
import pg from "pg";
import { buildApp } from "./app";
import { PostgresApiKeyStore } from "./auth/postgres-api-key-store";
import { StoredApiKeyAuthenticator } from "./auth/api-keys";
import { loadConfig } from "./config";
import { OllamaProvider } from "./providers/ollama";
import { RedisTokenBucket } from "./rate-limit/redis-token-bucket";

const { Pool } = pg;

const config = loadConfig();
const pool = new Pool({ connectionString: config.databaseUrl });
const redis = new Redis(config.redisUrl, {
  lazyConnect: true,
  maxRetriesPerRequest: 1,
});
const apiKeyStore = new PostgresApiKeyStore(pool);
const apiKeyAuthenticator = new StoredApiKeyAuthenticator(apiKeyStore);
const rateLimiter = new RedisTokenBucket(redis);
const provider = new OllamaProvider(
  config.ollamaBaseUrl,
  config.ollamaRequestTimeoutMs,
);
const app = buildApp({ provider, apiKeyAuthenticator, rateLimiter });
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
