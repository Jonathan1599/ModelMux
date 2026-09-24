import "dotenv/config";

export interface Config {
  port: number;
  ollamaBaseUrl: string;
  ollamaRequestTimeoutMs: number;
  databaseUrl: string;
  redisUrl: string;
  rabbitmqUrl: string;
  providerMaxConcurrency: number;
  providerMaxQueueSize: number;
  providerQueueTimeoutMs: number;
  jobConcurrency: number;
  jobMaxAttempts: number;
  jobBackoffMs: number;
}

export function loadConfig(env: NodeJS.ProcessEnv = process.env): Config {
  const config: Config = {
    port: parsePort(env.PORT ?? "3000"),
    ollamaBaseUrl: env.OLLAMA_BASE_URL?.trim() || "http://localhost:11434",
    ollamaRequestTimeoutMs: parsePositiveInteger(
      env.OLLAMA_REQUEST_TIMEOUT_MS ?? "120000",
      "OLLAMA_REQUEST_TIMEOUT_MS",
    ),
    databaseUrl:
      env.DATABASE_URL?.trim() ||
      "postgresql://modelmux:modelmux@localhost:5432/modelmux",
    redisUrl: env.REDIS_URL?.trim() || "redis://localhost:6379",
    rabbitmqUrl:
      env.RABBITMQ_URL?.trim() || "amqp://modelmux:modelmux@localhost:5672",
    providerMaxConcurrency: parsePositiveInteger(
      env.PROVIDER_MAX_CONCURRENCY ?? "2",
      "PROVIDER_MAX_CONCURRENCY",
    ),
    providerMaxQueueSize: parseNonNegativeInteger(
      env.PROVIDER_MAX_QUEUE_SIZE ?? "20",
      "PROVIDER_MAX_QUEUE_SIZE",
    ),
    providerQueueTimeoutMs: parsePositiveInteger(
      env.PROVIDER_QUEUE_TIMEOUT_MS ?? "30000",
      "PROVIDER_QUEUE_TIMEOUT_MS",
    ),
    jobConcurrency: parsePositiveInteger(
      env.JOB_CONCURRENCY ?? "2",
      "JOB_CONCURRENCY",
    ),
    jobMaxAttempts: parsePositiveInteger(
      env.JOB_MAX_ATTEMPTS ?? "3",
      "JOB_MAX_ATTEMPTS",
    ),
    jobBackoffMs: parsePositiveInteger(
      env.JOB_BACKOFF_MS ?? "1000",
      "JOB_BACKOFF_MS",
    ),
  };

  validateUrl(config.ollamaBaseUrl, "OLLAMA_BASE_URL", ["http:", "https:"]);
  validateUrl(config.databaseUrl, "DATABASE_URL", ["postgres:", "postgresql:"]);
  validateUrl(config.redisUrl, "REDIS_URL", ["redis:", "rediss:"]);
  validateUrl(config.rabbitmqUrl, "RABBITMQ_URL", ["amqp:", "amqps:"]);

  return config;
}

function parsePort(value: string): number {
  const port = Number(value);

  if (!Number.isInteger(port) || port < 1 || port > 65_535) {
    throw new Error(
      `PORT must be an integer between 1 and 65535; received ${value}`,
    );
  }

  return port;
}

function parsePositiveInteger(
  value: string,
  name: string,
): number {
  const parsed = Number(value);

  if (!Number.isInteger(parsed) || parsed < 1 || parsed > 2_147_483_647) {
    throw new Error(`${name} must be a positive integer; received ${value}`);
  }

  return parsed;
}

function parseNonNegativeInteger(
  value: string,
  name: string,
): number {
  const parsed = Number(value);

  if (!Number.isInteger(parsed) || parsed < 0 || parsed > 2_147_483_647) {
    throw new Error(
      `${name} must be a non-negative integer; received ${value}`,
    );
  }

  return parsed;
}

function validateUrl(
  value: string,
  name: string,
  allowedProtocols: readonly string[],
): void {
  let url: URL;

  try {
    url = new URL(value);
  } catch {
    throw new Error(`${name} must be a valid URL; received ${value}`);
  }

  if (!allowedProtocols.includes(url.protocol)) {
    throw new Error(
      `${name} must use ${allowedProtocols.join(" or ")}; received ${value}`,
    );
  }
}
