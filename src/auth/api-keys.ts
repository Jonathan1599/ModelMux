import { createHash } from "node:crypto";
import { AuthenticationError } from "../errors";
import type { RateLimitPolicy } from "../rate-limit/rate-limiter";

export interface ApiKeyPrincipal {
  id: string;
  name: string;
  keyPrefix: string;
  rateLimit: RateLimitPolicy;
}

export interface ApiKeyStore {
  findEnabledByHash(keyHash: string): Promise<ApiKeyPrincipal | null>;
}

export interface ApiKeyAuthenticator {
  authenticate(authorizationHeader: string | undefined): Promise<ApiKeyPrincipal>;
}

export class StoredApiKeyAuthenticator implements ApiKeyAuthenticator {
  public constructor(private readonly store: ApiKeyStore) {}

  public async authenticate(
    authorizationHeader: string | undefined,
  ): Promise<ApiKeyPrincipal> {
    const rawApiKey = parseBearerToken(authorizationHeader);
    const apiKey = await this.store.findEnabledByHash(hashApiKey(rawApiKey));

    if (!apiKey) {
      throw new AuthenticationError();
    }

    return apiKey;
  }
}

export function hashApiKey(rawApiKey: string): string {
  return createHash("sha256").update(rawApiKey, "utf8").digest("hex");
}

function parseBearerToken(header: string | undefined): string {
  const match = header?.match(/^Bearer\s+(\S+)$/i);

  if (!match?.[1]) {
    throw new AuthenticationError();
  }

  return match[1];
}
