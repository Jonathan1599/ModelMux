import { Counter, Gauge, Histogram, Registry } from "prom-client";
import type { ConcurrencySnapshot } from "../concurrency/concurrency-limiter";
import type { ProviderHealth } from "../providers/router";
import type { MetricEvent, MetricSink } from "./telemetry";

const durationBuckets = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 180, 300];
const routes = new Set(["/health", "/v1/chat", "/v1/chat/stream", "/v1/jobs", "/v1/jobs/:id", "/v1/providers"]);
const providerLabel = (name: string) => name === "ollama" || name === "openai" ? name : "other";
const seconds = (ms: number) => Math.max(0, ms) / 1000;
export type QueueDepth = { queued: number; retrying: number; active: number };

export class Metrics implements MetricSink {
  public readonly registry = new Registry();
  private readonly collectors: Array<() => void | Promise<void>> = [];
  private scraping: Promise<string> | undefined;
  private readonly httpActive = this.gauge("http_active", "HTTP requests currently in flight");
  private readonly httpRequests = this.counter("http_requests_total", "Finished HTTP transports, not SSE semantic outcomes", ["method", "route", "status", "outcome"]);
  private readonly httpDuration = this.histogram("http_duration_seconds", "HTTP transport lifetime including queueing and full streaming", ["method", "route", "outcome"]);
  private readonly rateLimited = this.counter("rate_limit_rejections_total", "Requests rejected by per-key rate limiting");
  private readonly cache = this.counter("cache_lookups_total", "Ordinary chat handler cache lookup decisions, including bypass", ["status"]);
  private readonly streams = this.counter("streams_total", "Streaming handler outcomes; HTTP 200 does not imply stream success", ["outcome"]);
  private readonly admissions = this.counter("admissions_total", "Process-local concurrency acquisition outcomes", ["outcome"]);
  private readonly queueWait = this.histogram("queue_wait_seconds", "Time waiting for gateway capacity, including unsuccessful waits", ["outcome"]);
  private readonly providerActive = this.gauge("provider_active", "Actual provider calls in flight", ["provider", "mode"]);
  private readonly providerCalls = this.counter("provider_calls_total", "Actual provider call outcomes, excluding circuit rejections", ["provider", "mode", "outcome"]);
  private readonly providerDuration = this.histogram("provider_duration_seconds", "Actual provider call lifetime, including stream consumer waits", ["provider", "mode", "outcome"]);
  private readonly circuitRejections = this.counter("circuit_rejections_total", "Calls skipped by open or half-open circuits", ["provider", "mode"]);
  private readonly fallbacks = this.counter("fallbacks_total", "Routing passes proceeding to the fallback target", ["from", "to"]);
  private readonly jobsActive = this.gauge("job_attempts_active", "Claimed attempts executing or persisting in this worker");
  private readonly jobs = this.counter("job_attempts_total", "Local attempt outcomes including result persistence; not distinct jobs", ["outcome"]);
  private readonly jobDuration = this.histogram("job_attempt_duration_seconds", "Claim-to-outcome attempt duration including inference and persistence", ["outcome"]);
  private readonly jobQueueWait = this.histogram("job_queue_wait_seconds", "Time from attempt availability until claim, excluding scheduled backoff");
  private readonly retries = this.counter("job_retries_started_total", "Claimed attempts whose attempt number exceeds one");
  private readonly collectorUp = this.gauge("collector_up", "Whether the last collection succeeded (1) or failed (0)", ["collector"]);

  public constructor(service: "gateway" | "worker") {
    this.registry.setDefaultLabels({ service });
  }

  public scrape(): Promise<string> {
    // Coalesce concurrent scrapes and finish snapshot collection before encoding
    // collector health; otherwise cross-metric collect callbacks can race.
    this.scraping ??= (async () => {
      await Promise.all(this.collectors.map((collect) => collect()));
      return this.registry.metrics();
    })().finally(() => { this.scraping = undefined; });
    return this.scraping;
  }

  public record(event: MetricEvent): void {
    switch (event.kind) {
      case "http_start": this.httpActive.inc(); break;
      case "http_end": {
        const labels = {
          method: ["GET", "POST", "HEAD", "OPTIONS"].includes(event.method) ? event.method : "OTHER",
          route: routes.has(event.route) ? event.route : "unmatched",
          outcome: event.outcome,
        };
        const status = event.status && event.status >= 100 && event.status <= 599 ? String(event.status) : "none";
        this.httpActive.dec();
        this.httpRequests.inc({ ...labels, status });
        this.httpDuration.observe(labels, seconds(event.durationMs));
        break;
      }
      case "rate_limit_rejection": this.rateLimited.inc(); break;
      case "cache": this.cache.inc({ status: event.status }); break;
      case "stream": this.streams.inc({ outcome: event.outcome }); break;
      case "admission":
        this.admissions.inc({ outcome: event.outcome });
        this.queueWait.observe({ outcome: event.outcome }, seconds(event.durationMs));
        break;
      case "provider_start": this.providerActive.inc({ provider: providerLabel(event.provider), mode: event.mode }); break;
      case "provider_end": {
        const labels = { provider: providerLabel(event.provider), mode: event.mode };
        this.providerActive.dec(labels);
        this.providerCalls.inc({ ...labels, outcome: event.outcome });
        this.providerDuration.observe({ ...labels, outcome: event.outcome }, seconds(event.durationMs));
        break;
      }
      case "circuit_rejection": this.circuitRejections.inc({ provider: providerLabel(event.provider), mode: event.mode }); break;
      case "fallback": this.fallbacks.inc({ from: providerLabel(event.from), to: providerLabel(event.to) }); break;
      case "job_start":
        this.jobsActive.inc();
        if (event.retry) this.retries.inc();
        if (event.queueWaitTimeMs !== undefined && Number.isFinite(event.queueWaitTimeMs)) this.jobQueueWait.observe(seconds(event.queueWaitTimeMs));
        break;
      case "job_end":
        this.jobsActive.dec();
        this.jobs.inc({ outcome: event.outcome });
        this.jobDuration.observe({ outcome: event.outcome }, seconds(event.durationMs));
        break;
    }
  }

  public bindConcurrency(snapshot: () => ConcurrencySnapshot): void {
    const active = this.gauge("concurrency_active", "Occupied synchronous provider permits");
    const queued = this.gauge("concurrency_queued", "Requests in the bounded gateway FIFO, not RabbitMQ");
    this.collectors.push(() => {
      try {
        const value = snapshot();
        active.set(value.active);
        queued.set(value.queued);
        this.collectorUp.set({ collector: "concurrency" }, 1);
      } catch {
        active.set(NaN);
        queued.set(NaN);
        this.collectorUp.set({ collector: "concurrency" }, 0);
      }
    });
  }

  public bindProviders(snapshot: () => ProviderHealth[]): void {
    const state = this.gauge("circuit_state", "One-hot process-local circuit state; snapshots never probe providers", ["provider", "state"]);
    this.collectors.push(() => {
      state.reset();
      try {
        for (const provider of snapshot()) {
          for (const name of ["closed", "open", "half-open"]) {
            state.set({ provider: providerLabel(provider.name), state: name }, Number(provider.state === name));
          }
        }
        this.collectorUp.set({ collector: "circuits" }, 1);
      } catch { this.collectorUp.set({ collector: "circuits" }, 0); }
    });
  }

  // Bind once in the gateway, never in every worker. Replicated gateway gauges
  // describe the same durable backlog and must be aggregated with max, not sum.
  public bindQueueDepth(read: () => Promise<QueueDepth>, timeoutMs = 1000): void {
    const depth = this.gauge("jobs_depth", "Shared Postgres nonterminal jobs, not RabbitMQ delivery count", ["status"]);
    this.collectors.push(async () => {
      let timer: NodeJS.Timeout | undefined;
      try {
        const data = await Promise.race([
          read(),
          new Promise<never>((_resolve, reject) => { timer = setTimeout(() => reject(new Error("Collection timed out")), timeoutMs); }),
        ]);
        for (const status of ["queued", "retrying", "active"] as const) depth.set({ status }, data[status]);
        this.collectorUp.set({ collector: "jobs" }, 1);
      } catch {
        // Do not turn a failed query into a misleading zero or stale healthy gauge.
        for (const status of ["queued", "retrying", "active"]) depth.set({ status }, NaN);
        this.collectorUp.set({ collector: "jobs" }, 0);
      } finally { clearTimeout(timer); }
    });
  }

  private counter(name: string, help: string, labelNames: string[] = []) {
    return new Counter({ name: `modelmux_${name}`, help, labelNames, registers: [this.registry] });
  }
  private gauge(name: string, help: string, labelNames: string[] = []) {
    return new Gauge({ name: `modelmux_${name}`, help, labelNames, registers: [this.registry] });
  }
  private histogram(name: string, help: string, labelNames: string[] = []) {
    return new Histogram({ name: `modelmux_${name}`, help, labelNames, buckets: durationBuckets, registers: [this.registry] });
  }
}
