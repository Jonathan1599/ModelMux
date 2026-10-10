export type CacheStatus = "HIT" | "MISS" | "BYPASS";
export type ProviderOutcome = "success" | "transient_error" | "permanent_error" | "cancelled" | "consumer_error";
export type JobOutcome = "success" | "retrying" | "failed" | "obsolete" | "infrastructure_error";
export type MetricEvent =
  | { kind: "http_start" }
  | { kind: "http_end"; method: string; route: string; status: number | undefined; outcome: "completed" | "aborted"; durationMs: number }
  | { kind: "rate_limit_rejection" }
  | { kind: "cache"; status: CacheStatus }
  | { kind: "stream"; outcome: "completed" | "failed" | "cancelled" }
  | { kind: "admission"; outcome: "admitted" | "queue_full" | "timeout" | "cancelled"; durationMs: number }
  | { kind: "provider_start"; provider: string; mode: "chat" | "stream" }
  | { kind: "provider_end"; provider: string; mode: "chat" | "stream"; outcome: ProviderOutcome; durationMs: number }
  | { kind: "circuit_rejection"; provider: string; mode: "chat" | "stream" }
  | { kind: "fallback"; from: string; to: string }
  | { kind: "job_start"; retry: boolean; queueWaitTimeMs?: number | undefined }
  | { kind: "job_end"; outcome: JobOutcome; durationMs: number };

export interface MetricSink {
  record(event: MetricEvent): void;
}

// Instrumentation is best effort: it must never change admission, fallback,
// persistence, acknowledgements, or cancellation if a metric implementation fails.
export function recordMetric(sink: MetricSink | undefined, event: MetricEvent): void {
  try { sink?.record(event); } catch { /* Drop telemetry, not application work. */ }
}
