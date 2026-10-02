import assert from "node:assert/strict";
import test from "node:test";
import {
  CircuitBreaker,
  CircuitOpenError,
} from "../src/providers/circuit-breaker";

const transientError = new Error("provider unavailable");
const permanentError = new Error("invalid request");
const isFailure = (error: unknown): boolean => error === transientError;

function setup(failureThreshold = 2) {
  let now = 0;
  const breaker = new CircuitBreaker({
    failureThreshold,
    resetTimeoutMs: 1_000,
    now: () => now,
  });

  return {
    breaker,
    advance: (milliseconds: number) => { now += milliseconds; },
  };
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

async function fail(breaker: CircuitBreaker): Promise<void> {
  await assert.rejects(
    breaker.execute(async () => { throw transientError; }, isFailure),
    (error) => error === transientError,
  );
}

test("circuit starts closed and opens only after consecutive failures", async () => {
  const { breaker } = setup();
  assert.deepEqual(breaker.snapshot(), {
    state: "closed", consecutiveFailures: 0, retryAfterMs: 0,
  });

  await fail(breaker);
  assert.equal(breaker.snapshot().consecutiveFailures, 1);
  assert.equal(breaker.snapshot().state, "closed");

  assert.equal(await breaker.execute(async () => "answer", isFailure), "answer");
  assert.equal(breaker.snapshot().consecutiveFailures, 0);

  await fail(breaker);
  await fail(breaker);
  assert.deepEqual(breaker.snapshot(), {
    state: "open", consecutiveFailures: 2, retryAfterMs: 1_000,
  });
});

test("open circuit rejects without calling the provider and reports remaining cooldown", async () => {
  const { breaker, advance } = setup(1);
  await fail(breaker);
  advance(250);
  let calls = 0;

  await assert.rejects(breaker.execute(async () => { calls += 1; }, isFailure), (error) => {
    assert.ok(error instanceof CircuitOpenError);
    assert.equal(error.retryAfterMs, 750);
    return true;
  });
  assert.equal(calls, 0);
  assert.equal(breaker.snapshot().retryAfterMs, 750);
});

test("cooldown admits exactly one half-open probe and success closes the circuit", async () => {
  const { breaker, advance } = setup(1);
  await fail(breaker);
  advance(1_000);
  assert.deepEqual(breaker.snapshot(), {
    state: "open", consecutiveFailures: 1, retryAfterMs: 0,
  });

  const provider = deferred<string>();
  const probe = breaker.execute(() => provider.promise, isFailure);
  assert.equal(breaker.snapshot().state, "half-open");
  let calls = 0;
  await Promise.all(Array.from({ length: 5 }, () => assert.rejects(
    breaker.execute(async () => { calls += 1; }, isFailure),
    CircuitOpenError,
  )));
  assert.equal(calls, 0);

  provider.resolve("recovered");
  assert.equal(await probe, "recovered");
  assert.deepEqual(breaker.snapshot(), {
    state: "closed", consecutiveFailures: 0, retryAfterMs: 0,
  });
  assert.equal(await breaker.execute(async () => "next", isFailure), "next");
});

test("a failed half-open probe starts a fresh cooldown", async () => {
  const { breaker, advance } = setup(1);
  await fail(breaker);
  advance(1_000);
  const provider = deferred<void>();
  const probe = assert.rejects(
    breaker.execute(() => provider.promise, isFailure),
    (error) => error === transientError,
  );

  advance(250);
  provider.reject(transientError);
  await probe;
  assert.deepEqual(breaker.snapshot(), {
    state: "open", consecutiveFailures: 2, retryAfterMs: 1_000,
  });
  advance(999);
  await assert.rejects(breaker.execute(async () => undefined, isFailure), CircuitOpenError);
  advance(1);
  await breaker.execute(async () => undefined, isFailure);
  assert.equal(breaker.snapshot().state, "closed");
});

test("permanent errors reset failure counts and still reach the caller", async () => {
  const { breaker } = setup();
  await fail(breaker);
  await assert.rejects(
    breaker.execute(async () => { throw permanentError; }, isFailure),
    (error) => error === permanentError,
  );
  assert.deepEqual(breaker.snapshot(), {
    state: "closed", consecutiveFailures: 0, retryAfterMs: 0,
  });
});

test("a permanent error from the half-open probe closes the circuit", async () => {
  const { breaker, advance } = setup(1);
  await fail(breaker);
  advance(1_000);
  await assert.rejects(
    breaker.execute(async () => { throw permanentError; }, isFailure),
    (error) => error === permanentError,
  );
  assert.deepEqual(breaker.snapshot(), {
    state: "closed", consecutiveFailures: 0, retryAfterMs: 0,
  });
});

test("late success cannot close a circuit opened by another request", async () => {
  const { breaker } = setup(1);
  const provider = deferred<string>();
  const earlierCall = breaker.execute(() => provider.promise, isFailure);
  await fail(breaker);

  provider.resolve("late answer");
  assert.equal(await earlierCall, "late answer");
  assert.deepEqual(breaker.snapshot(), {
    state: "open", consecutiveFailures: 1, retryAfterMs: 1_000,
  });
});

test("late failure cannot extend an open circuit cooldown", async () => {
  const { breaker, advance } = setup(1);
  const provider = deferred<void>();
  const earlierCall = assert.rejects(
    breaker.execute(() => provider.promise, isFailure),
    (error) => error === transientError,
  );
  await fail(breaker);
  advance(500);

  provider.reject(transientError);
  await earlierCall;
  assert.deepEqual(breaker.snapshot(), {
    state: "open", consecutiveFailures: 1, retryAfterMs: 500,
  });
});

test("late success cannot release an active half-open probe", async () => {
  const { breaker, advance } = setup(1);
  const oldProvider = deferred<void>();
  const earlierCall = breaker.execute(() => oldProvider.promise, isFailure);
  await fail(breaker);
  advance(1_000);
  const probeProvider = deferred<void>();
  const probe = breaker.execute(() => probeProvider.promise, isFailure);

  oldProvider.resolve();
  await earlierCall;
  assert.equal(breaker.snapshot().state, "half-open");
  await assert.rejects(breaker.execute(async () => undefined, isFailure), CircuitOpenError);

  probeProvider.resolve();
  await probe;
  assert.equal(breaker.snapshot().state, "closed");
});

test("late failure cannot reopen a circuit after a successful recovery probe", async () => {
  const { breaker, advance } = setup(1);
  const provider = deferred<void>();
  const earlierCall = assert.rejects(
    breaker.execute(() => provider.promise, isFailure),
    (error) => error === transientError,
  );
  await fail(breaker);
  advance(1_000);
  await breaker.execute(async () => undefined, isFailure);

  provider.reject(transientError);
  await earlierCall;
  assert.deepEqual(breaker.snapshot(), {
    state: "closed", consecutiveFailures: 0, retryAfterMs: 0,
  });
});

test("circuit validates thresholds and cooldown durations", () => {
  for (const invalid of [0, -1, 1.5, Number.NaN, Infinity, Number.MAX_SAFE_INTEGER + 1]) {
    assert.throws(() => new CircuitBreaker({ failureThreshold: invalid, resetTimeoutMs: 1_000 }));
    assert.throws(() => new CircuitBreaker({ failureThreshold: 2, resetTimeoutMs: invalid }));
  }
});
