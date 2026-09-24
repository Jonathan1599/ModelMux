import pino from "pino";
import pg from "pg";
import { loadConfig } from "./config";
import { createInferenceProcessor } from "./jobs/inference-processor";
import { INFERENCE_QUEUE_NAME } from "./jobs/inference-jobs";
import { OllamaProvider } from "./providers/ollama";
import { RabbitMqJobWorker } from "./queue/rabbitmq";
import { RabbitMqBroker } from "./queue/rabbitmq-broker";
import { PostgresJobStore } from "./queue/postgres-job-store";
import type { JobWorker } from "./queue/job-queue";
import type { ChatRequest, ChatResponse } from "./types/chat";

const config = loadConfig();
const logger = pino({ name: "modelmux-worker" });
const provider = new OllamaProvider(config.ollamaBaseUrl, config.ollamaRequestTimeoutMs);
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
  leaseMs: config.ollamaRequestTimeoutMs + 30_000,
});
const broker = new RabbitMqBroker({
  name: INFERENCE_QUEUE_NAME,
  url: config.rabbitmqUrl,
  concurrency: config.jobConcurrency,
});
const worker: JobWorker = new RabbitMqJobWorker(
  broker, store, createInferenceProcessor(provider, logger), logger,
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
    await pool.end().catch((err: unknown) => {
      logger.error({ err }, "Failed to close the Postgres pool");
      process.exitCode = 1;
    });
  }
}

process.once("SIGINT", () => void shutdown());
process.once("SIGTERM", () => void shutdown());

void worker.run().catch(async (err: unknown) => {
  logger.fatal({ err }, "Inference worker stopped unexpectedly");
  process.exitCode = 1;
  await shutdown();
});
