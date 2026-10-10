import assert from "node:assert/strict";
import test from "node:test";
import pino from "pino";
import { IdempotencyConflictError, JobNotFoundError, JobQueueUnavailableError } from "../src/errors";
import { RabbitMqJobQueue, RabbitMqJobWorker } from "../src/queue/rabbitmq";
import { JobProcessingError } from "../src/queue/job-error";
import type { RabbitBroker } from "../src/queue/rabbitmq-broker";
import type { JobEnvelope } from "../src/queue/postgres-job-store";
import type { ProcessingJob } from "../src/queue/job-queue";
import { Metrics } from "../src/observability/metrics";
import type { MetricSink } from "../src/observability/telemetry";
import { metricSample as sample } from "./helpers/metric-sample";

const job: ProcessingJob<string> = {
  id: "job-1", requestId: "request-1", ownerId: "owner-1", data: "hello", attempt: 1,
};
const envelope: JobEnvelope = { id: job.id, attempt: 1 };
const logger = pino({ enabled: false });

test("RabbitMQ queue preserves application errors and maps storage outages to 503", async () => {
  for (const error of [new JobNotFoundError(), new IdempotencyConflictError(), new Error("private database error")]) {
    const queue = new RabbitMqJobQueue<string, string>({
      async enqueue() { throw error; },
      async getJob() { throw error; },
    });
    const expected = error instanceof JobNotFoundError || error instanceof IdempotencyConflictError
      ? error : JobQueueUnavailableError;
    await assert.rejects(queue.enqueue({ ...job, idempotencyKey: "key" }), expected);
    await assert.rejects(queue.getJob(job.id, job.ownerId), expected);
  }
});

type Store = ConstructorParameters<typeof RabbitMqJobWorker<string, string>>[1];

async function startWorker(store: Partial<Store>, processor: (job: ProcessingJob<string>) => Promise<string>, metrics?: MetricSink) {
  let handler!: (message: JobEnvelope) => Promise<"ack" | "dead-letter">;
  let ready!: () => void;
  const connected = new Promise<void>((resolve) => { ready = resolve; });
  const events: string[] = [];
  const broker: RabbitBroker = {
    failure: undefined,
    async connect() {},
    async publish() {},
    async consume(callback) { handler = callback; ready(); },
    async stopConsuming() { events.push("cancel"); },
    async close() { events.push("close"); },
  };
  const worker = new RabbitMqJobWorker(broker, {
    async dispatchOne() { return false; },
    async maintain() {},
    async claim() { return job; },
    async complete() {},
    async fail() { return "failed"; },
    ...store,
  }, processor, logger, metrics);
  const running = worker.run();
  await connected;
  return { handler, events, async close() { await worker.close(); await running; } };
}

test("worker persists the provider result before acknowledging delivery", async (t) => {
  const events: string[] = [];
  const worker = await startWorker({
    async complete(message, result) {
      assert.deepEqual(message, envelope);
      assert.equal(result, "result");
      events.push("persist");
    },
  }, async (received) => { assert.deepEqual(received, job); events.push("infer"); return "result"; });
  t.after(worker.close);
  assert.equal(await worker.handler(envelope), "ack");
  assert.deepEqual(events, ["infer", "persist"]);
});

test("duplicate or obsolete deliveries do not invoke inference", async (t) => {
  const worker = await startWorker({ async claim() { return null; } }, async () => {
    throw new Error("Processor must not run for duplicate delivery");
  });
  t.after(worker.close);
  assert.equal(await worker.handler(envelope), "ack");
});

test("retry scheduling is persisted before acknowledging the failed attempt", async (t) => {
  let persisted = false;
  const worker = await startWorker({
    async fail(message, error) {
      assert.deepEqual(message, envelope);
      assert.equal(error.retryable, true);
      persisted = true;
      return "retrying";
    },
  }, async () => { throw new JobProcessingError("PROVIDER_UNAVAILABLE", true); });
  t.after(worker.close);
  assert.equal(await worker.handler(envelope), "ack");
  assert.equal(persisted, true);
});

test("terminal failures are persisted before dead-lettering; unknown failures are sanitized", async (t) => {
  let persisted = false;
  const worker = await startWorker({
    async fail(_message, error) {
      assert.equal(error.code, "JOB_FAILED");
      assert.equal(error.retryable, false);
      persisted = true;
      return "failed";
    },
  }, async () => { throw new Error("private provider detail"); });
  t.after(worker.close);
  assert.equal(await worker.handler(envelope), "dead-letter");
  assert.equal(persisted, true);
});

test("a database failure after inference prevents acknowledgement without being reclassified", async (t) => {
  const dbError = new Error("database unavailable");
  const worker = await startWorker({
    async complete() { throw dbError; },
    async fail() { assert.fail("Database errors must not be recorded as provider errors"); },
  }, async () => "result");
  t.after(worker.close);
  await assert.rejects(worker.handler(envelope), dbError);
});

test("closing a worker cancels consumption and shuts down its dispatcher", async () => {
  const worker = await startWorker({}, async () => "result");
  await worker.close();
  await worker.close();
  assert.deepEqual(worker.events, ["cancel", "close"]);
});

test("job metrics track claimed retries and persistence outcomes without changing acknowledgements", async (t) => {
  for (const outcome of ["success", "retrying", "failed", "infrastructure_error"] as const) {
    const metrics = new Metrics("worker");
    const worker = await startWorker({
      async claim() { return { ...job, attempt: 2, queueWaitTimeMs: 125 }; },
      async complete() { if (outcome === "infrastructure_error") throw new Error("database offline"); },
      async fail() { return outcome === "retrying" ? "retrying" : "failed"; },
    }, async () => {
      assert.equal(await sample(metrics, "job_attempts_active"), 1);
      if (outcome === "retrying" || outcome === "failed") throw new JobProcessingError("JOB_FAILED", outcome === "retrying");
      return "answer";
    }, metrics);
    t.after(worker.close);
    if (outcome === "infrastructure_error") await assert.rejects(worker.handler(envelope), /database offline/);
    else assert.equal(await worker.handler(envelope), outcome === "failed" ? "dead-letter" : "ack");
    assert.equal(await sample(metrics, "job_attempts_total", { outcome }), 1);
    assert.equal(await sample(metrics, "job_retries_started_total"), 1);
    assert.equal(await sample(metrics, "job_attempts_active"), 0);
    assert.equal(await sample(metrics, "job_queue_wait_seconds", {}, "_sum"), 0.125);
  }
});

test("duplicate delivery is not an attempt and throwing metrics cannot break persistence-before-ack", async (t) => {
  const metrics = new Metrics("worker");
  const duplicate = await startWorker({ async claim() { return null; } }, async () => "unused", metrics);
  t.after(duplicate.close);
  await duplicate.handler(envelope);
  assert.equal(await sample(metrics, "job_attempt_duration_seconds", {}, "_count"), 0);
  let persisted = false;
  const worker = await startWorker({ async complete() { persisted = true; } }, async () => "answer", {
    record() { throw new Error("broken telemetry"); },
  });
  t.after(worker.close);
  assert.equal(await worker.handler(envelope), "ack");
  assert.equal(persisted, true);
});
