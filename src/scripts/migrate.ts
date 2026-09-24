import { readFile, readdir } from "node:fs/promises";
import { resolve } from "node:path";
import pg from "pg";
import { loadConfig } from "../config";

const { Pool } = pg;

async function main(): Promise<void> {
  const config = loadConfig();
  const pool = new Pool({ connectionString: config.databaseUrl });

  try {
    const directory = resolve("db/migrations");
    const files = (await readdir(directory)).filter((file) => file.endsWith(".sql")).sort();
    for (const file of files) {
      await pool.query(await readFile(resolve(directory, file), "utf8"));
    }
    console.log("Database migration completed.");
  } catch (error) {
    console.error("Database migration failed.", error);
    process.exitCode = 1;
  } finally {
    await pool.end();
  }
}

void main();
