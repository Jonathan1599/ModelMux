import assert from "node:assert/strict";
import test from "node:test";
import { setTimeout as delay } from "node:timers/promises";
import { IdempotencyConflictError, JobNotFoundError } from "../../src/errors";
import { PostgresJobStore } from "../../src/queue/postgres-job-store";
import { JobProcessingError } from "../../src/queue/job-error";
import { testDatabase } from "./database";

test("Postgres job store: atomic submissions, outbox, fencing, recovery, retention", { timeout: 15_000 }, async (t) => {
  const database = await testDatabase();
  t.after(database.close);
  const { pool } = database;
  const options = { name: "test", maxAttempts: 2, backoffMs: 1_000, leaseMs: 60_000 };
  const store = new PostgresJobStore<{ a: number; b: number }, string>(pool, options);
  const submission = { ownerId: "owner-1", requestId: "request-1", idempotencyKey: "key-1", data: { a: 1, b: 2 } };
  const receipts = await Promise.all(Array.from({ length: 10 }, () => store.enqueue(submission)));
  const { id } = receipts[0]!;
  assert.equal(new Set(receipts.map((receipt) => receipt.id)).size, 1);
  assert.deepEqual(await store.enqueue({ ...submission, data: { b: 2, a: 1 } }), receipts[0]);
  await assert.rejects(store.enqueue({ ...submission, data: { a: 2, b: 1 } }), IdempotencyConflictError);
  await assert.rejects(store.getJob(id, "another-owner"), JobNotFoundError);

  await assert.rejects(store.dispatchOne(async () => { throw new Error("unconfirmed"); }), /unconfirmed/);
  assert.equal((await pool.query("SELECT dispatch_pending FROM inference_jobs WHERE id = $1", [id])).rows[0].dispatch_pending, true);
  let publications = 0;
  await Promise.all(Array.from({ length: 3 }, () => store.dispatchOne(async (message) => {
    assert.deepEqual(message, { id, attempt: 1 });
    publications += 1;
    await delay(20);
  })));
  assert.equal(publications, 1);
  const claims = await Promise.all([store.claim({ id, attempt: 1 }), store.claim({ id, attempt: 1 })]);
  assert.equal(claims.filter(Boolean).length, 1);
  assert.equal((await store.getJob(id, submission.ownerId)).status, "active");

  assert.equal(await store.fail({ id, attempt: 1 }, new JobProcessingError("PROVIDER_TIMEOUT", true)), "retrying");
  assert.equal(await store.claim({ id, attempt: 2 }), null);
  await pool.query("UPDATE inference_jobs SET available_at = NOW() - INTERVAL '1 second' WHERE id = $1", [id]);
  assert.ok(await store.claim({ id, attempt: 2 }));
  await store.complete({ id, attempt: 1 }, "stale result");
  assert.equal((await store.getJob(id, submission.ownerId)).status, "active");
  assert.equal(await store.fail({ id, attempt: 2 }, new JobProcessingError("PROVIDER_TIMEOUT", true)), "failed");
  assert.equal((await store.getJob(id, submission.ownerId)).error?.code, "PROVIDER_TIMEOUT");

  const recovery = await store.enqueue({ ...submission, idempotencyKey: "recover" });
  await store.claim({ id: recovery.id, attempt: 1 });
  await pool.query("UPDATE inference_jobs SET lease_until = NOW() - INTERVAL '1 second' WHERE id = $1", [recovery.id]);
  await store.maintain();
  assert.equal((await store.getJob(recovery.id, submission.ownerId)).status, "queued");
  assert.ok(await store.claim({ id: recovery.id, attempt: 2 }));
  await store.complete({ id: recovery.id, attempt: 1 }, "stale");
  await store.complete({ id: recovery.id, attempt: 2 }, "correct");
  assert.equal((await store.getJob(recovery.id, submission.ownerId)).response, "correct");

  await pool.query("UPDATE inference_jobs SET finished_at = NOW() - INTERVAL '25 hours' WHERE id = $1", [id]);
  await store.maintain();
  await assert.rejects(store.getJob(id, submission.ownerId), JobNotFoundError);
  const fresh = await store.enqueue(submission);
  assert.notEqual(fresh.id, id);
  assert.equal(await store.claim({ id, attempt: 1 }), null);
});
