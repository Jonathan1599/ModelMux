import assert from "node:assert/strict";
import test from "node:test";
import { RequestAbortedError, throwIfCancelled } from "../src/cancellation";
import { InMemoryConcurrencyLimiter } from "../src/concurrency/in-memory-concurrency-limiter";
import {
  ConcurrencyQueueFullError,
  ConcurrencyWaitTimeoutError,
} from "../src/errors";

test("concurrency limiter admits queued work in FIFO order", async () => {
  const limiter = new InMemoryConcurrencyLimiter({
    maxConcurrent: 1,
    maxQueueSize: 2,
    waitTimeoutMs: 1_000,
  });
  const first = await limiter.acquire();
  const admissionOrder: string[] = [];
  const secondPromise = limiter.acquire().then((permit) => {
    admissionOrder.push("second");
    return permit;
  });
  const thirdPromise = limiter.acquire().then((permit) => {
    admissionOrder.push("third");
    return permit;
  });

  assert.equal(limiter.snapshot().active, 1);
  assert.equal(limiter.snapshot().queued, 2);

  first.release();
  const second = await secondPromise;
  assert.deepEqual(admissionOrder, ["second"]);
  assert.equal(limiter.snapshot().active, 1);
  assert.equal(limiter.snapshot().queued, 1);

  second.release();
  const third = await thirdPromise;
  assert.deepEqual(admissionOrder, ["second", "third"]);

  third.release();
  third.release();

  const snapshot = limiter.snapshot();
  assert.equal(snapshot.active, 0);
  assert.equal(snapshot.queued, 0);
  assert.equal(snapshot.admitted, 3);
  assert.ok(snapshot.totalWaitTimeMs >= 0);
  assert.ok(snapshot.maxWaitTimeMs >= 0);
});

test("concurrency limiter rejects work when its wait queue is full", async () => {
  const limiter = new InMemoryConcurrencyLimiter({
    maxConcurrent: 1,
    maxQueueSize: 1,
    waitTimeoutMs: 1_000,
  });
  const active = await limiter.acquire();
  const queuedPromise = limiter.acquire();

  await assert.rejects(limiter.acquire(), ConcurrencyQueueFullError);
  assert.equal(limiter.snapshot().rejected, 1);

  active.release();
  const queued = await queuedPromise;
  queued.release();
});

test("concurrency limiter times out queued work and removes it", async () => {
  const limiter = new InMemoryConcurrencyLimiter({
    maxConcurrent: 1,
    maxQueueSize: 1,
    waitTimeoutMs: 10,
  });
  const active = await limiter.acquire();

  await assert.rejects(limiter.acquire(), ConcurrencyWaitTimeoutError);

  const snapshot = limiter.snapshot();
  assert.equal(snapshot.active, 1);
  assert.equal(snapshot.queued, 0);
  assert.equal(snapshot.timedOut, 1);

  active.release();
  assert.equal(limiter.snapshot().active, 0);
});

test("aborted waiters leave the FIFO queue and do not consume the next permit", async () => {
  const limiter = new InMemoryConcurrencyLimiter({ maxConcurrent: 1, maxQueueSize: 2, waitTimeoutMs: 1000 });
  const active = await limiter.acquire();
  const controller = new AbortController();
  const cancelled = assert.rejects(limiter.acquire(controller.signal), RequestAbortedError);
  const next = limiter.acquire();
  controller.abort();
  await cancelled;
  assert.equal(limiter.snapshot().queued, 1);
  assert.equal(limiter.snapshot().aborted, 1);
  assert.equal(limiter.snapshot().timedOut, 0);
  assert.equal(limiter.snapshot().admitted, 1);
  active.release();
  const permit = await next;
  permit.release();
  permit.release();
  assert.equal(limiter.snapshot().active, 0);
  assert.equal(limiter.snapshot().queued, 0);
  assert.equal(limiter.snapshot().admitted, 2);
});

test("cancellation before acquire and immediately after admission never leaks capacity", async () => {
  const limiter = new InMemoryConcurrencyLimiter({ maxConcurrent: 1, maxQueueSize: 1, waitTimeoutMs: 1000 });
  const alreadyAborted = AbortSignal.abort();
  await assert.rejects(limiter.acquire(alreadyAborted), RequestAbortedError);
  assert.equal(limiter.snapshot().active, 0);
  assert.equal(limiter.snapshot().admitted, 0);
  const controller = new AbortController();
  const admitted = limiter.acquire(controller.signal);
  controller.abort();
  const permit = await admitted;
  try { assert.throws(() => throwIfCancelled(controller.signal), RequestAbortedError); }
  finally { permit.release(); }
  assert.equal(limiter.snapshot().active, 0);
});
