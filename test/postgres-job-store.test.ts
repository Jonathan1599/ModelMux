import assert from "node:assert/strict";
import test from "node:test";
import type { Pool } from "pg";
import { PostgresJobStore } from "../src/queue/postgres-job-store";

function fixture() {
  const statements: string[] = [];
  let released = false;
  const client = {
    async query(sql: string) {
      statements.push(sql.trim());
      return { rows: sql.includes("SELECT id") ? [{ id: "job-1", attempts_made: 2 }] : [] };
    },
    release() { released = true; },
  };
  const pool = { async connect() { return client; } } as unknown as Pool;
  return {
    statements,
    get released() { return released; },
    store: new PostgresJobStore<string, string>(pool, {
      name: "test", maxAttempts: 3, backoffMs: 100, leaseMs: 1_000,
    }),
  };
}

test("outbox does not clear pending publication or commit before confirmation", async () => {
  const { store, statements } = fixture();
  let confirm!: () => void;
  let publishing!: () => void;
  const started = new Promise<void>((resolve) => { publishing = resolve; });
  const confirmed = new Promise<void>((resolve) => { confirm = resolve; });
  const dispatching = store.dispatchOne(async (message) => {
    assert.deepEqual(message, { id: "job-1", attempt: 3 });
    publishing();
    await confirmed;
  });
  await started;
  assert.equal(statements[0], "BEGIN");
  assert.ok(!statements.includes("COMMIT"));
  assert.ok(!statements.some((sql) => sql.startsWith("UPDATE")));
  confirm();
  assert.equal(await dispatching, true);
  assert.match(statements[2]!, /dispatch_pending = FALSE/);
  assert.equal(statements[3], "COMMIT");
});

test("an unconfirmed publish rolls back and releases the connection for a later retry", async () => {
  const state = fixture();
  const error = new Error("publish timed out");
  await assert.rejects(state.store.dispatchOne(async () => { throw error; }), error);
  assert.equal(state.statements.at(-1), "ROLLBACK");
  assert.ok(!state.statements.some((sql) => sql.startsWith("UPDATE")));
  assert.equal(state.released, true);
});
