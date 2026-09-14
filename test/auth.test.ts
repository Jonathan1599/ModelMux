import assert from "node:assert/strict";
import test from "node:test";
import {
  hashApiKey,
  StoredApiKeyAuthenticator,
  type ApiKeyPrincipal,
  type ApiKeyStore,
} from "../src/auth/api-keys";
import { PostgresApiKeyStore } from "../src/auth/postgres-api-key-store";
import {
  ApiKeyStoreUnavailableError,
  AuthenticationError,
} from "../src/errors";
import type { Pool } from "pg";

const principal: ApiKeyPrincipal = {
  id: "key-1",
  name: "test key",
  keyPrefix: "mm_test",
  rateLimit: { capacity: 10, refillPerSecond: 1 },
};

test("API key authentication hashes the bearer token before lookup", async () => {
  let receivedHash: string | undefined;
  const store: ApiKeyStore = {
    async findEnabledByHash(keyHash) {
      receivedHash = keyHash;
      return principal;
    },
  };
  const authenticator = new StoredApiKeyAuthenticator(store);

  const result = await authenticator.authenticate("Bearer secret-api-key");

  assert.equal(result, principal);
  assert.equal(receivedHash, hashApiKey("secret-api-key"));
  assert.notEqual(receivedHash, "secret-api-key");
});

test("API key authentication rejects malformed and unknown credentials", async () => {
  let storeCalls = 0;
  const store: ApiKeyStore = {
    async findEnabledByHash() {
      storeCalls += 1;
      return null;
    },
  };
  const authenticator = new StoredApiKeyAuthenticator(store);

  await assert.rejects(authenticator.authenticate(undefined), AuthenticationError);
  await assert.rejects(
    authenticator.authenticate("Basic abc123"),
    AuthenticationError,
  );
  await assert.rejects(
    authenticator.authenticate("Bearer unknown-key"),
    AuthenticationError,
  );
  assert.equal(storeCalls, 1);
});

test("Postgres key lookup fails closed when the database is unavailable", async () => {
  const pool = {
    async query() {
      throw new Error("Postgres is unavailable");
    },
  } as unknown as Pool;
  const store = new PostgresApiKeyStore(pool);

  await assert.rejects(
    store.findEnabledByHash(hashApiKey("secret-api-key")),
    ApiKeyStoreUnavailableError,
  );
});
