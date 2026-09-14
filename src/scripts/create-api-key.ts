import { randomBytes, randomUUID } from "node:crypto";
import pg from "pg";
import { hashApiKey } from "../auth/api-keys";
import { loadConfig } from "../config";

const { Pool } = pg;

async function main(): Promise<void> {
  const [name, capacityInput = "10", refillInput = "1"] = process.argv.slice(2);

  if (!name?.trim()) {
    console.error(
      "Usage: npm run api-key:create -- <name> [capacity] [refill-per-second]",
    );
    process.exitCode = 1;
    return;
  }

  const config = loadConfig();
  const pool = new Pool({ connectionString: config.databaseUrl });

  try {
    const capacity = parseCapacity(capacityInput);
    const refillPerSecond = parseRefillRate(refillInput);
    const rawApiKey = `mm_${randomBytes(32).toString("base64url")}`;
    const keyPrefix = rawApiKey.slice(0, 12);

    await pool.query(
      `INSERT INTO api_keys (
         id,
         name,
         key_prefix,
         key_hash,
         rate_limit_capacity,
         rate_limit_refill_per_second
       ) VALUES ($1, $2, $3, $4, $5, $6)`,
      [
        randomUUID(),
        name.trim(),
        keyPrefix,
        hashApiKey(rawApiKey),
        capacity,
        refillPerSecond,
      ],
    );

    console.log(`Created API key "${name.trim()}" (${keyPrefix}…).`);
    console.log(`API key (shown once): ${rawApiKey}`);
  } catch (error) {
    console.error("Failed to create API key.", error);
    process.exitCode = 1;
  } finally {
    await pool.end();
  }
}

void main();

function parseCapacity(value: string): number {
  const parsed = Number(value);

  if (!Number.isInteger(parsed) || parsed < 1) {
    throw new Error(`Capacity must be a positive integer; received ${value}`);
  }

  return parsed;
}

function parseRefillRate(value: string): number {
  const parsed = Number(value);

  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`Refill rate must be positive; received ${value}`);
  }

  return parsed;
}
