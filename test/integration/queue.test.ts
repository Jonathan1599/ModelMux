import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import test from "node:test";
import { connect, type ChannelModel } from "amqplib";
import pino from "pino";
import { loadConfig } from "../../src/config";
import { ProviderConnectionError, ProviderHttpError } from "../../src/errors";
import { createInferenceProcessor } from "../../src/jobs/inference-processor";
import { RabbitMqJobQueue, RabbitMqJobWorker } from "../../src/queue/rabbitmq";
import { RabbitMqBroker } from "../../src/queue/rabbitmq-broker";
import { PostgresJobStore } from "../../src/queue/postgres-job-store";
import type { JobStatus } from "../../src/queue/job-queue";
import type { ChatRequest, ChatResponse } from "../../src/types/chat";
import { testDatabase } from "./database";

test("RabbitMQ integration: deduplication, retries, dead letters, results, and bounded consumption", { timeout: 25_000 }, async (t) => {
  const database = await testDatabase();
  const name = `modelmux-test-${randomUUID()}`;
  const url = loadConfig().rabbitmqUrl;
  const workers: RabbitMqJobWorker<ChatRequest, ChatResponse>[] = [];
  const runs: Promise<void>[] = [];
  let connection: ChannelModel | undefined;
  t.after(async () => {
    try {
      await Promise.all(workers.map((worker) => worker.close()));
      await Promise.all(runs);
      if (connection) {
        const channel = await connection.createChannel();
        await channel.deleteQueue(name);
        await channel.deleteQueue(`${name}.failed`);
      }
    } finally {
      try { await connection?.close(); } finally { await database.close(); }
    }
  });
  connection = await connect(url, { timeout: 1_500 });
  const store = new PostgresJobStore<ChatRequest, ChatResponse>(database.pool, {
    name, maxAttempts: 3, backoffMs: 100, leaseMs: 10_000,
  });
  const jobs = new RabbitMqJobQueue(store);
  const errors: unknown[] = [];
  const logger = pino({ enabled: false });
  const calls = new Map<string, number>();
  const attemptTimes: number[] = [];
  let active = 0;
  let peakActive = 0;
  const processor = createInferenceProcessor({
    async chat(data) {
      const count = (calls.get(data.model) ?? 0) + 1;
      calls.set(data.model, count);
      active += 1;
      peakActive = Math.max(peakActive, active);
      try {
        await delay(50);
        if (data.model === "retry") {
          attemptTimes.push(Date.now());
          if (count < 3) throw new ProviderConnectionError("Ollama");
        }
        if (data.model === "unavailable") throw new ProviderConnectionError("Ollama");
        if (data.model === "bad-model") throw new ProviderHttpError("Ollama", 404, "sensitive detail");
        return { message: { role: "assistant", content: data.model } };
      } finally {
        active -= 1;
      }
    },
  }, logger);
  const submission = (model: string) => ({
    ownerId: "owner-1", requestId: `request-${model}`, idempotencyKey: model,
    data: { model, messages: [{ role: "user" as const, content: "Hello" }] },
  });
  const duplicates = await Promise.all(Array.from({ length: 10 }, () => jobs.enqueue(submission("one"))));
  const first = duplicates[0]!;
  assert.equal(new Set(duplicates.map(({ id }) => id)).size, 1);
  assert.equal((await jobs.getJob(first.id, "owner-1")).status, "queued");
  const receipts = await Promise.all(["retry", "bad-model", "unavailable", "two", "three"].map((model) => jobs.enqueue(submission(model))));
  for (let index = 0; index < 2; index += 1) {
    const broker = new RabbitMqBroker({ name, url, concurrency: 2 });
    const worker = new RabbitMqJobWorker(broker, store, processor, logger);
    workers.push(worker);
    runs.push(worker.run().catch((error: unknown) => { errors.push(error); }));
  }
  async function terminal(id: string): Promise<JobStatus<ChatResponse>> {
    const deadline = Date.now() + 15_000;
    while (Date.now() < deadline) {
      const status = await jobs.getJob(id, "owner-1");
      if (status.status === "completed" || status.status === "failed") return status;
      await delay(20);
    }
    throw new Error(`Job ${id} did not finish`);
  }
  const results = await Promise.all(receipts.map(({ id }) => terminal(id)));
  assert.equal((await terminal(first.id)).response?.message.content, "one");
  assert.equal(calls.get("one"), 1);
  assert.equal(results[0]?.status, "completed");
  assert.equal(results[0]?.attemptsMade, 3);
  assert.equal(results[1]?.status, "failed");
  assert.equal(results[1]?.attemptsMade, 1);
  assert.equal(results[2]?.status, "failed");
  assert.equal(results[2]?.attemptsMade, 3);
  assert.doesNotMatch(JSON.stringify(results), /sensitive detail/);
  assert.ok(attemptTimes[1]! - attemptTimes[0]! >= 100);
  assert.ok(attemptTimes[2]! - attemptTimes[1]! >= 200);
  assert.ok(peakActive <= 2, `Concurrency exceeded: ${peakActive}`);
  assert.equal(active, 0);
  const channel = await connection.createChannel();
  let deadLetters = 0;
  for (let count = 0; count < 100 && deadLetters < 2; count += 1) {
    deadLetters = (await channel.checkQueue(`${name}.failed`)).messageCount;
    await delay(20);
  }
  assert.equal(deadLetters, 2);
  assert.deepEqual(errors, []);
});
