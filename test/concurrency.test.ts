import assert from "node:assert/strict";
import test from "node:test";
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
