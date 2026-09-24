// Only these public descriptions are exposed by job polling. Broker errors,
// stack traces, and upstream response bodies never become API error messages.
const publicErrors = {
  PROVIDER_UNAVAILABLE: "Unable to connect to the inference provider",
  PROVIDER_TIMEOUT: "Inference provider request timed out",
  PROVIDER_ERROR: "Inference provider request failed",
  INVALID_PROVIDER_RESPONSE: "Inference provider returned an invalid response",
  JOB_FAILED: "Inference job failed",
};

export type JobErrorCode = keyof typeof publicErrors;

export class JobProcessingError extends Error {
  public constructor(
    public readonly code: JobErrorCode,
    public readonly retryable: boolean,
  ) {
    super(code);
    this.name = "JobProcessingError";
  }
}

export function publicJobError(reason: string): { code: JobErrorCode; message: string } {
  const code = Object.hasOwn(publicErrors, reason)
    ? reason as JobErrorCode
    : "JOB_FAILED";
  return { code, message: publicErrors[code] };
}
