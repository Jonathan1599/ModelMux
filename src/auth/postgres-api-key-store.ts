import type { Pool } from "pg";
import { ApiKeyStoreUnavailableError } from "../errors.js";
import type { ApiKeyPrincipal, ApiKeyStore } from "./api-keys.js";

interface ApiKeyRow {
  id: string;
  name: string;
  key_prefix: string;
  rate_limit_capacity: number;
  rate_limit_refill_per_second: number;
}

export class PostgresApiKeyStore implements ApiKeyStore {
  public constructor(private readonly pool: Pool) {}

  public async findEnabledByHash(
    keyHash: string,
  ): Promise<ApiKeyPrincipal | null> {
    let row: ApiKeyRow | undefined;

    try {
      const result = await this.pool.query<ApiKeyRow>(
        `SELECT
           id,
           name,
           key_prefix,
           rate_limit_capacity,
           rate_limit_refill_per_second
         FROM api_keys
         WHERE key_hash = $1 AND enabled = true
         LIMIT 1`,
        [keyHash],
      );
      row = result.rows[0];
    } catch (cause) {
      throw new ApiKeyStoreUnavailableError(cause);
    }

    if (!row) {
      return null;
    }

    const capacity = Number(row.rate_limit_capacity);
    const refillPerSecond = Number(row.rate_limit_refill_per_second);

    if (
      !Number.isInteger(capacity) ||
      capacity < 1 ||
      !Number.isFinite(refillPerSecond) ||
      refillPerSecond <= 0
    ) {
      throw new ApiKeyStoreUnavailableError(
        new Error(`API key ${row.id} has invalid rate limit metadata`),
      );
    }

    return {
      id: row.id,
      name: row.name,
      keyPrefix: row.key_prefix,
      rateLimit: { capacity, refillPerSecond },
    };
  }
}
