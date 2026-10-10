import fastify from "fastify";
import type { Metrics } from "./metrics";

// Separate from the public API and its database-backed authentication. Bind to
// loopback on the host, or an unpublished port inside the Compose network.
export function buildMetricsServer(metrics: Metrics) {
  const app = fastify({ logger: false });
  app.get("/metrics", async (_request, response) => {
    void response.header("cache-control", "no-store");
    try {
      const body = await metrics.scrape();
      return response.type(metrics.registry.contentType).send(body);
    } catch {
      return response.status(503).send({ error: "Metrics collection failed" });
    }
  });
  return app;
}
