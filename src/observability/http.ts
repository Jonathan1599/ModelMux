import type { FastifyInstance } from "fastify";
import { recordMetric, type MetricSink } from "./telemetry";

export function observeHttp(app: FastifyInstance, metrics: MetricSink): void {
  app.addHook("onRequest", async (request, response) => {
    const startedAt = performance.now();
    recordMetric(metrics, { kind: "http_start" });
    let finished = false;
    const finish = (outcome: "completed" | "aborted") => {
      if (finished) return;
      finished = true;
      response.raw.off("finish", onFinish);
      response.raw.off("close", onClose);
      recordMetric(metrics, {
        kind: "http_end", method: request.method, route: request.routeOptions.url ?? "unmatched",
        status: response.raw.headersSent ? response.raw.statusCode : undefined,
        outcome,
        durationMs: performance.now() - startedAt,
      });
    };
    const onFinish = () => finish("completed");
    const onClose = () => finish(response.raw.writableFinished ? "completed" : "aborted");
    // Covers normal replies, hijacked SSE, auth/validation failures and disconnects.
    response.raw.once("finish", onFinish);
    response.raw.once("close", onClose);
  });
}
