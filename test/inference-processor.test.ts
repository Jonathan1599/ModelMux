import assert from "node:assert/strict";
import test from "node:test";
import pino from "pino";
import { ProviderConnectionError, ProviderHttpError, ProviderResponseError, ProviderTimeoutError } from "../src/errors";
import { createInferenceProcessor } from "../src/jobs/inference-processor";
import { JobProcessingError } from "../src/queue/job-error";
import type { ProcessingJob } from "../src/queue/job-queue";
import type { ChatRequest, ChatResponse } from "../src/types/chat";

const job: ProcessingJob<ChatRequest> = {
  id: "job-1", ownerId: "owner-1", requestId: "request-1", attempt: 1,
  data: { model: "llama3.2", messages: [{ role: "user", content: "Hello" }] },
};
const logger = pino({ enabled: false });

test("inference processor calls the provider and preserves the original request ID in logs", async () => {
  const logs: string[] = [];
  const log = pino({}, { write(line) { logs.push(line); } });
  const response: ChatResponse = { message: { role: "assistant", content: "hello back" } };
  const processor = createInferenceProcessor({
    async chat(data) {
      assert.deepEqual(data, job.data);
      return response;
    },
  }, log);
  assert.deepEqual(await processor(job), response);
  for (const line of logs) {
    const entry = JSON.parse(line);
    assert.equal(entry.reqId, job.requestId);
    assert.equal(entry.jobId, job.id);
    assert.equal(entry.apiKeyId, job.ownerId);
    assert.equal(entry.attempt, 1);
    assert.equal(entry.data, undefined);
  }
  assert.equal(logs.length, 2);
});

test("transient provider failures request a retry without exposing upstream details", async () => {
  for (const error of [
    new ProviderConnectionError("Ollama"),
    new ProviderTimeoutError("Ollama", 100),
    new ProviderHttpError("Ollama", 429, "private detail"),
    new ProviderHttpError("Ollama", 503, "private detail"),
  ]) {
    const processor = createInferenceProcessor({ async chat() { throw error; } }, logger);
    await assert.rejects(processor(job), (failure: unknown) => {
      assert.ok(failure instanceof JobProcessingError);
      assert.equal(failure.retryable, true);
      assert.equal(failure.code, error.code);
      assert.doesNotMatch(failure.message, /private detail/);
      return true;
    });
  }
});

test("permanent and unexpected failures stop retries and expose only safe codes", async () => {
  for (const [error, code] of [
    [new ProviderHttpError("Ollama", 404, "missing model"), "PROVIDER_ERROR"],
    [new ProviderResponseError("Ollama"), "INVALID_PROVIDER_RESPONSE"],
    [new Error("secret connection string"), "JOB_FAILED"],
  ] as const) {
    const processor = createInferenceProcessor({ async chat() { throw error; } }, logger);
    await assert.rejects(processor(job), (failure: unknown) => {
      assert.ok(failure instanceof JobProcessingError);
      assert.equal(failure.retryable, false);
      assert.equal(failure.code, code);
      assert.doesNotMatch(failure.message, /secret|missing model/);
      return true;
    });
  }
});
