import assert from "node:assert/strict";
import test from "node:test";
import { Metrics } from "../../src/observability/metrics";
import { buildMetricsServer } from "../../src/observability/server";
import { chatApp, chatHeaders, chatPayload } from "../helpers/chat-app";

test("real HTTP: private metrics listener scrapes independently and does not expose metrics on the API port", { timeout: 5000 }, async (t) => {
  const metrics = new Metrics("gateway");
  const { app, limitRequests } = chatApp({ async chat() { return { message: { role: "assistant", content: "answer" } }; } }, 2, metrics);
  const metricsApp = buildMetricsServer(metrics);
  t.after(async () => { await app.close(); await metricsApp.close(); });
  const base = await app.listen({ host: "127.0.0.1", port: 0 });
  const internal = await metricsApp.listen({ host: "127.0.0.1", port: 0 });
  const answer = await fetch(base + "/v1/chat", {
    method: "POST", headers: { ...chatHeaders, "content-type": "application/json" }, body: JSON.stringify(chatPayload),
  });
  await answer.text();
  limitRequests();
  const response = await fetch(internal + "/metrics");
  assert.equal(response.status, 200);
  assert.match(response.headers.get("content-type")!, /text\/plain/);
  const text = await response.text();
  assert.match(text, /modelmux_http_requests_total\{[^\n]+status="200"[^\n]+\} 1/);
  assert.match(text, /modelmux_http_active\{[^\n]+\} 0/);
  assert.equal((await fetch(base + "/metrics")).status, 404);
});
