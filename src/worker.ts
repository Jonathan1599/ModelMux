import pino from "pino";
import pg from "pg";
import { loadConfig, providerExecutionTimeoutMs } from "./config";
import { createInferenceProcessor } from "./jobs/inference-processor";
import { INFERENCE_QUEUE_NAME } from "./jobs/inference-jobs";
import { createProvider } from "./providers/create-provider";
import { RabbitMqJobWorker } from "./queue/rabbitmq";
import { RabbitMqBroker } from "./queue/rabbitmq-broker";
import { PostgresJobStore } from "./queue/postgres-job-store";
import type { JobWorker } from "./queue/job-queue";
import type { ChatRequest, ChatResponse } from "./types/chat";
import { Metrics } from "./observability/metrics";
import { buildMetricsServer } from "./observability/server";

const config = loadConfig();
const logger = pino({ name: "modelmux-worker" });
const metrics = new Metrics("worker");
const provider = createProvider(config, metrics);
metrics.bindProviders(() => provider.snapshot());
const metricsServer = buildMetricsServer(metrics);
const pool = new pg.Pool({
  connectionString: config.databaseUrl,
  connectionTimeoutMillis: 5_000,
  statement_timeout: 10_000,
});
pool.on("error", (err) => logger.error({ err }, "Idle Postgres client failed"));
const store = new PostgresJobStore<ChatRequest, ChatResponse>(pool, {
  name: INFERENCE_QUEUE_NAME,
  maxAttempts: config.jobMaxAttempts,
  backoffMs: config.jobBackoffMs,
  leaseMs: providerExecutionTimeoutMs(config) + 30_000,
});
const broker = new RabbitMqBroker({
  name: INFERENCE_QUEUE_NAME,
  url: config.rabbitmqUrl,
  concurrency: config.jobConcurrency,
});
const worker: JobWorker = new RabbitMqJobWorker(
  broker, store, createInferenceProcessor(provider, logger), logger, metrics,
);
let isShuttingDown = false;

async function shutdown(): Promise<void> {
  if (isShuttingDown) return;
  isShuttingDown = true;
  logger.info("Draining active jobs and shutting down worker");
  try {
    await worker.close();
  } catch (err) {
    logger.error({ err }, "Failed to close worker cleanly");
    process.exitCode = 1;
  } finally {
    await metricsServer.close().catch(() => logger.warn("Metrics listener failed to close cleanly"));
    await pool.end().catch((err: unknown) => {
      logger.error({ err }, "Failed to close the Postgres pool");
      process.exitCode = 1;
    });
  }
}

process.once("SIGINT", () => void shutdown());
process.once("SIGTERM", () => void shutdown());

async function start(): Promise<void> {
  await metricsServer.listen({ port: config.metrics.workerPort, host: config.metrics.host })
    .catch(() => logger.warn("Metrics listener unavailable; worker remains running"));
  if (!isShuttingDown) await worker.run();
}

void start().catch(async (err: unknown) => {
  logger.fatal({ err }, "Inference worker stopped unexpectedly");
  process.exitCode = 1;
  await shutdown();
});
