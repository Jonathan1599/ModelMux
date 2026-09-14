import { readFile } from "node:fs/promises";
import { resolve } from "node:path";
import pg from "pg";
import { loadConfig } from "../config";

const { Pool } = pg;

async function main(): Promise<void> {
  const config = loadConfig();
  const pool = new Pool({ connectionString: config.databaseUrl });

  try {
    const migration = await readFile(
      resolve("db/migrations/001_create_api_keys.sql"),
      "utf8",
    );
    await pool.query(migration);
    console.log("Database migration completed.");
  } catch (error) {
    console.error("Database migration failed.", error);
    process.exitCode = 1;
  } finally {
    await pool.end();
  }
}

void main();
