import assert from "node:assert/strict";
import { createServer, request as httpRequest, type RequestListener } from "node:http";
import { setTimeout as delay } from "node:timers/promises";
import test, { type TestContext } from "node:test";
import { OllamaProvider } from "../../src/providers/ollama";
import { chatApp, chatHeaders, chatPayload } from "../helpers/chat-app";

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => { resolve = done; });
  return { promise, resolve };
}

async function until(check: () => boolean) {
  for (let count = 0; count < 200; count += 1) {
    if (check()) return;
    await delay(5);
  }
  assert.fail("Timed out waiting for expected lifecycle state");
}

async function setup(t: TestContext, handle: RequestListener, maxQueueSize = 2) {
  const upstream = createServer(handle);
  await new Promise<void>((resolve, reject) => {
    upstream.once("error", reject);
    upstream.listen(0, "127.0.0.1", resolve);
  });
  t.after(async () => {
    upstream.closeAllConnections();
    await new Promise<void>((resolve) => upstream.close(() => resolve()));
  });
  const address = upstream.address();
  assert.ok(address && typeof address !== "string");
  const fixture = chatApp(new OllamaProvider(`http://127.0.0.1:${address.port}`, 5000), maxQueueSize);
  t.after(async () => { fixture.app.server.closeAllConnections(); await fixture.app.close(); });
  const base = await fixture.app.listen({ port: 0, host: "127.0.0.1" });
  return { ...fixture, base };
}

function startRequest(base: string, path: string) {
  const request = httpRequest(base + path, {
    method: "POST", headers: { ...chatHeaders, "content-type": "application/json" },
  });
  // Intentional client disconnects produce ECONNRESET before response headers.
  request.on("error", () => {});
  request.on("response", (response) => { response.on("error", () => {}); response.resume(); });
  request.end(JSON.stringify(chatPayload));
  return request;
}

test("real HTTP delivers a delta before upstream completion and preserves the SSE terminal event", { timeout: 5000 }, async (t) => {
  const finish = deferred();
  t.after(finish.resolve);
  const { base, limiter } = await setup(t, (_request, response) => {
    response.writeHead(200, { "content-type": "application/x-ndjson" });
    response.write('{"message":{"content":"Hello"},"done":false}\n');
    void finish.promise.then(() => response.end('{"message":{"content":" world"},"done":true}\n'));
  });
  const response = await fetch(base + "/v1/chat/stream", {
    method: "POST", headers: { ...chatHeaders, "content-type": "application/json" }, body: JSON.stringify(chatPayload),
  });
  assert.equal(response.status, 200);
  const reader = response.body!.getReader();
  const first = await reader.read();
  const firstText = new TextDecoder().decode(first.value);
  assert.match(firstText, /event: delta/);
  assert.doesNotMatch(firstText, /event: done/);
  assert.equal(limiter.snapshot().active, 1);
  finish.resolve();
  let remaining = "";
  for (;;) {
    const chunk = await reader.read();
    if (chunk.done) break;
    remaining += new TextDecoder().decode(chunk.value);
  }
  assert.match(remaining, /event: done/);
  await until(() => limiter.snapshot().active === 0);
});

test("real client disconnect aborts Ollama and frees capacity for both chat modes", { timeout: 8000 }, async (t) => {
  for (const path of ["/v1/chat", "/v1/chat/stream"]) {
    const started = deferred();
    const closed = deferred();
    let calls = 0;
    const { base, limiter, app } = await setup(t, (_request, response) => {
      calls += 1;
      if (calls === 1) {
        response.on("close", closed.resolve);
        if (path.endsWith("stream")) response.write('{"message":{"content":"partial"},"done":false}\n');
        started.resolve();
      } else response.end('{"message":{"role":"assistant","content":"next"},"done":true}\n');
    });
    const client = startRequest(base, path);
    t.after(() => client.destroy());
    await started.promise;
    client.destroy();
    await closed.promise;
    await until(() => limiter.snapshot().active === 0);
    const next = await app.inject({ method: "POST", url: "/v1/chat", headers: chatHeaders, payload: chatPayload });
    assert.equal(next.statusCode, 200);
    assert.equal(next.json().message.content, "next");
    assert.equal(calls, 2);
  }
});

test("disconnect while queued removes the waiter without calling the provider", { timeout: 5000 }, async (t) => {
  const started = deferred();
  let calls = 0;
  const { base, limiter } = await setup(t, () => { calls += 1; started.resolve(); });
  const active = startRequest(base, "/v1/chat/stream");
  t.after(() => active.destroy());
  await started.promise;
  const queued = startRequest(base, "/v1/chat/stream");
  t.after(() => queued.destroy());
  await until(() => limiter.snapshot().queued === 1);
  queued.destroy();
  await until(() => limiter.snapshot().queued === 0);
  assert.equal(limiter.snapshot().aborted, 1);
  assert.equal(limiter.snapshot().timedOut, 0);
  active.destroy();
  await until(() => limiter.snapshot().active === 0);
  assert.equal(calls, 1);
});

test("gateway shutdown cancels active SSE and queued work before closing", { timeout: 5000 }, async (t) => {
  const started = deferred();
  const upstreamClosed = deferred();
  const warnings: Error[] = [];
  const onWarning = (warning: Error) => { warnings.push(warning); };
  process.on("warning", onWarning);
  t.after(() => { process.off("warning", onWarning); });
  const { base, app, limiter } = await setup(t, (_request, response) => {
    response.on("close", upstreamClosed.resolve);
    response.write('{"message":{"content":"partial"},"done":false}\n');
    started.resolve();
  }, 20);
  const active = startRequest(base, "/v1/chat/stream");
  t.after(() => active.destroy());
  await started.promise;
  const queued = Array.from({ length: 12 }, () => startRequest(base, "/v1/chat"));
  t.after(() => { queued.forEach((request) => request.destroy()); });
  await until(() => limiter.snapshot().queued === queued.length);
  await app.close();
  await upstreamClosed.promise;
  assert.equal(limiter.snapshot().active, 0);
  assert.equal(limiter.snapshot().queued, 0);
  assert.equal(limiter.snapshot().aborted, queued.length);
  assert.equal(warnings.filter((warning) => warning.name === "MaxListenersExceededWarning").length, 0);
});
