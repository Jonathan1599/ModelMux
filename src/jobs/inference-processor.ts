import type { Logger } from "pino";
import {
  ProviderConnectionError,
  ProviderHttpError,
  ProviderResponseError,
  ProviderTimeoutError,
} from "../errors";
import type { LLMProvider } from "../providers/provider";
import { JobProcessingError } from "../queue/job-error";
import type { JobProcessor } from "../queue/job-queue";
import type { ChatRequest, ChatResponse } from "../types/chat";

export function createInferenceProcessor(
  provider: LLMProvider,
  logger: Logger,
): JobProcessor<ChatRequest, ChatResponse> {
  return async (job) => {
    const log = logger.child({
      reqId: job.requestId,
      jobId: job.id,
      apiKeyId: job.ownerId,
      attempt: job.attempt,
    });
    const startedAt = performance.now();
    log.info("Inference job started");

    try {
      const response = await provider.chat(job.data);
      log.info(
        { processingTimeMs: performance.now() - startedAt },
        "Inference job completed",
      );
      return response;
    } catch (error) {
      log.error({ err: error }, "Inference job attempt failed");

      if (
        error instanceof ProviderConnectionError ||
        error instanceof ProviderTimeoutError
      ) {
        throw new JobProcessingError(error.code, true);
      }
      if (error instanceof ProviderHttpError) {
        throw new JobProcessingError(
          error.code,
          error.upstreamStatus === 429 || error.upstreamStatus >= 500,
        );
      }
      if (error instanceof ProviderResponseError) {
        throw new JobProcessingError(error.code, false);
      }
      throw new JobProcessingError("JOB_FAILED", false);
    }
  };
}
