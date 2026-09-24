import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import pg from "pg";
import { loadConfig } from "../../src/config";

export async function testDatabase() {
  // Use the configured local Postgres, isolated by a random schema. Never
  // create or operate a Postgres container, or touch the application's tables.
  const schema = `modelmux_test_${randomUUID().replaceAll("-", "")}`;
  const pool = new pg.Pool({
    connectionString: loadConfig().databaseUrl,
    options: `-c search_path=${schema}`,
    connectionTimeoutMillis: 1_500,
    statement_timeout: 5_000,
  });
  let created = false;
  async function close() {
    try {
      if (created) await pool.query(`DROP SCHEMA ${schema} CASCADE`);
    } finally {
      await pool.end();
    }
  }
  try {
    await pool.query(`CREATE SCHEMA ${schema}`);
    created = true;
    await pool.query(await readFile("db/migrations/002_create_inference_jobs.sql", "utf8"));
    return { pool, close };
  } catch (error) {
    await close();
    throw error;
  }
}
