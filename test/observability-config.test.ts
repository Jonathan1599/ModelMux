import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { resolve } from "node:path";
import test from "node:test";
import { InMemoryConcurrencyLimiter } from "../src/concurrency/in-memory-concurrency-limiter";
import { Metrics } from "../src/observability/metrics";

const read = (path: string) => readFile(resolve(__dirname, "..", path), "utf8");

test("provisioned dashboard references registered metrics and the configured datasource", async () => {
  // Static contract check, not a PromQL parser or a live Grafana render test.
  const dashboard = JSON.parse(await read("observability/grafana/dashboards/modelmux.json")) as {
    uid: string;
    panels: Array<{ id: number; datasource: { uid: string }; targets: Array<{ expr: string }> }>;
  };
  const datasource = await read("observability/grafana/provisioning/datasources/prometheus.yml");
  const provisioning = await read("observability/grafana/provisioning/dashboards/modelmux.yml");
  const prometheus = await read("observability/prometheus.yml");
  const compose = await read("docker-compose.yml");
  assert.equal(dashboard.uid, "modelmux-overview");
  assert.equal(new Set(dashboard.panels.map((panel) => panel.id)).size, dashboard.panels.length);
  assert.match(datasource, /uid: modelmux-prometheus/);
  assert.match(datasource, /url: http:\/\/prometheus:9090/);
  assert.match(provisioning, /path: \/var\/lib\/grafana\/dashboards/);
  assert.match(compose, /\.\/observability\/grafana\/dashboards:\/var\/lib\/grafana\/dashboards:ro/);
  for (const [service, port] of [["gateway", 9464], ["worker", 9465]] as const) {
    assert.ok(prometheus.includes(`${service}:${port}`));
    assert.ok(compose.includes(`${service.toUpperCase()}_METRICS_PORT: ${port}`));
  }

  const metrics = new Metrics("gateway");
  const limiter = new InMemoryConcurrencyLimiter({ maxConcurrent: 1, maxQueueSize: 1, waitTimeoutMs: 100 });
  metrics.bindConcurrency(() => limiter.snapshot());
  metrics.bindProviders(() => []);
  metrics.bindQueueDepth(async () => ({ queued: 0, retrying: 0, active: 0 }));
  const families = new Set((await metrics.registry.getMetricsAsJSON()).map((metric) => metric.name));
  const queries: string[] = [];
  for (const panel of dashboard.panels) {
    assert.equal(panel.datasource.uid, "modelmux-prometheus");
    for (const { expr } of panel.targets) {
      queries.push(expr);
      for (const [name] of expr.matchAll(/modelmux_[a-z_]+/g)) {
        assert.ok(families.has(name) || families.has(name.replace(/_(bucket|sum|count)$/, "")), `Unknown dashboard metric: ${name}`);
      }
      if (expr.includes("histogram_quantile")) assert.match(expr, /sum by \(le(?:,|\))/);
    }
  }
  const backlogQueries = queries.filter((query) => query.includes("modelmux_jobs_depth"));
  assert.equal(backlogQueries.length, 1);
  assert.match(backlogQueries[0]!, /^max by \(status\)/); // Shared state is not additive across gateways.
});
