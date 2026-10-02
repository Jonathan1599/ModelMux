import {
  ProviderCircuitOpenError,
  ProviderConnectionError,
  ProviderHttpError,
  ProviderTimeoutError,
} from "../errors";

// Keep fallback and async retry decisions aligned. Bad requests, malformed
// responses, and unexpected application errors are not transient failures.
export function isRetryableProviderError(error: unknown): error is
  ProviderCircuitOpenError | ProviderConnectionError | ProviderHttpError | ProviderTimeoutError {
  return error instanceof ProviderCircuitOpenError
    || error instanceof ProviderConnectionError
    || error instanceof ProviderTimeoutError
    || (error instanceof ProviderHttpError
      && (error.upstreamStatus === 429 || error.upstreamStatus >= 500));
}
