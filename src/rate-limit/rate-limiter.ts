export interface RateLimitPolicy {
  capacity: number;
  refillPerSecond: number;
}

export interface RateLimitDecision {
  allowed: boolean;
  remaining: number;
  retryAfterSeconds: number;
}

export interface RateLimiter {
  consume(subject: string, policy: RateLimitPolicy): Promise<RateLimitDecision>;
}
