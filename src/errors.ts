interface AppErrorOptions extends ErrorOptions {
  headers?: Readonly<Record<string, string>>;
}

export abstract class AppError extends Error {
  public abstract readonly statusCode: number;
  public abstract readonly code: string;
  public readonly headers: Readonly<Record<string, string>>;

  protected constructor(message: string, options: AppErrorOptions = {}) {
    super(message, options);
    this.name = new.target.name;
    this.headers = options.headers ?? {};
  }
}

export class ProviderHttpError extends AppError {
  public readonly statusCode = 502;
  public readonly code = "PROVIDER_ERROR";

  public constructor(
    public readonly provider: string,
    public readonly upstreamStatus: number,
    public readonly upstreamDetail: string | undefined,
  ) {
    super(`${provider} request failed`);
  }
}

export class ProviderConnectionError extends AppError {
  public readonly statusCode = 503;
  public readonly code = "PROVIDER_UNAVAILABLE";

  public constructor(provider: string, cause?: unknown) {
    super(`Unable to connect to ${provider}`, { cause });
  }
}

export class ProviderResponseError extends AppError {
  public readonly statusCode = 502;
  public readonly code = "INVALID_PROVIDER_RESPONSE";

  public constructor(provider: string, cause?: unknown) {
    super(`${provider} returned an invalid response`, { cause });
  }
}

export class ProviderTimeoutError extends AppError {
  public readonly statusCode = 504;
  public readonly code = "PROVIDER_TIMEOUT";

  public constructor(
    provider: string,
    public readonly timeoutMs: number,
    cause?: unknown,
  ) {
    super(`${provider} request timed out`, { cause });
  }
}

export class AuthenticationError extends AppError {
  public readonly statusCode = 401;
  public readonly code = "INVALID_API_KEY";

  public constructor() {
    super("Missing or invalid API key", {
      headers: { "www-authenticate": 'Bearer realm="modelmux"' },
    });
  }
}

export class ApiKeyStoreUnavailableError extends AppError {
  public readonly statusCode = 503;
  public readonly code = "API_KEY_STORE_UNAVAILABLE";

  public constructor(cause?: unknown) {
    super("API key service is unavailable", { cause });
  }
}

export class RateLimitExceededError extends AppError {
  public readonly statusCode = 429;
  public readonly code = "RATE_LIMIT_EXCEEDED";

  public constructor(public readonly retryAfterSeconds: number) {
    super("Rate limit exceeded", {
      headers: { "retry-after": String(retryAfterSeconds) },
    });
  }
}

export class RateLimiterUnavailableError extends AppError {
  public readonly statusCode = 503;
  public readonly code = "RATE_LIMITER_UNAVAILABLE";

  public constructor(cause?: unknown) {
    super("Rate limiter is unavailable", { cause });
  }
}

export class ConcurrencyQueueFullError extends AppError {
  public readonly statusCode = 503;
  public readonly code = "CONCURRENCY_QUEUE_FULL";

  public constructor(
    public readonly maxConcurrent: number,
    public readonly maxQueueSize: number,
  ) {
    super("Provider execution queue is full", {
      headers: { "retry-after": "1" },
    });
  }
}

export class ConcurrencyWaitTimeoutError extends AppError {
  public readonly statusCode = 503;
  public readonly code = "CONCURRENCY_WAIT_TIMEOUT";

  public constructor(public readonly waitTimeoutMs: number) {
    super("Timed out waiting for provider capacity", {
      headers: { "retry-after": "1" },
    });
  }
}

export class JobQueueUnavailableError extends AppError {
  public readonly statusCode = 503;
  public readonly code = "JOB_QUEUE_UNAVAILABLE";

  public constructor(cause?: unknown) {
    super("Inference job queue is unavailable", {
      cause,
      headers: { "retry-after": "1" },
    });
  }
}

export class JobNotFoundError extends AppError {
  public readonly statusCode = 404;
  public readonly code = "JOB_NOT_FOUND";

  public constructor() {
    super("Job not found");
  }
}

export class IdempotencyConflictError extends AppError {
  public readonly statusCode = 409;
  public readonly code = "IDEMPOTENCY_CONFLICT";

  public constructor() {
    super("Idempotency key has already been used with a different request");
  }
}
