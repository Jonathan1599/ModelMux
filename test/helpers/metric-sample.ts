import type { Metrics } from "../../src/observability/metrics";

export async function metricSample(metrics: Metrics, name: string, labels: Record<string, string> = {}, suffix = ""): Promise<number> {
  const data = await metrics.registry.getSingleMetric(`modelmux_${name}`)?.get();
  const value = data?.values.find((sample) => ("metricName" in sample ? sample.metricName : data.name) === `modelmux_${name}${suffix}`
    && Object.entries(labels).every(([key, expected]) => sample.labels[key] === expected));
  return value?.value ?? 0;
}
