export abstract class AppError extends Error {
  public abstract readonly statusCode: number;
  public abstract readonly code: string;

  protected constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = new.target.name;
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
