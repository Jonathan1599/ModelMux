export interface ConcurrencyPermit {
  waitTimeMs: number;
  activeAtAdmission: number;
  queuedAtAdmission: number;
  release(): void;
}

export interface ConcurrencySnapshot {
  active: number;
  queued: number;
  admitted: number;
  rejected: number;
  timedOut: number;
  totalWaitTimeMs: number;
  maxWaitTimeMs: number;
}

export interface ConcurrencyLimiter {
  acquire(): Promise<ConcurrencyPermit>;
  snapshot(): ConcurrencySnapshot;
}
