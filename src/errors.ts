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
    detail?: string,
  ) {
    const suffix = detail ? `: ${detail}` : "";
    super(`${provider} returned HTTP ${upstreamStatus}${suffix}`);
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
