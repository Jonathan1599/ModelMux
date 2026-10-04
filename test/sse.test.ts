import assert from "node:assert/strict";
import { EventEmitter, getEventListeners } from "node:events";
import type { ServerResponse } from "node:http";
import test from "node:test";
import { RequestAbortedError } from "../src/cancellation";
import { ProviderConnectionError } from "../src/errors";
import { writeSse } from "../src/routes/sse";
import { chatApp, chatHeaders, chatPayload } from "./helpers/chat-app";

test("SSE writer waits for drain, JSON-escapes content, and cleans up listeners", async () => {
  const socket = new EventEmitter();
  let frame = "";
  const response = Object.assign(socket, { destroyed: false, write(value: string) { frame = value; return false; } });
  const controller = new AbortController();
  let finished = false;
  const writing = writeSse(response as unknown as ServerResponse, "delta", { content: "one\ntwo" }, controller.signal)
    .then(() => { finished = true; });
  await Promise.resolve();
  assert.equal(finished, false);
  assert.equal(frame, 'event: delta\ndata: {"content":"one\\ntwo"}\n\n');
  socket.emit("drain");
  await writing;
  assert.equal(finished, true);
  assert.equal(socket.eventNames().length, 0);
  assert.equal(getEventListeners(controller.signal, "abort").length, 0);
});

test("SSE writer terminates drain waits on abort, close, error, and stalled-client timeout", async () => {
  for (const reason of ["abort", "close", "error", "timeout"]) {
    const socket = new EventEmitter();
    const response = Object.assign(socket, { destroyed: false, write() { return false; } });
    const controller = new AbortController();
    const writing = assert.rejects(writeSse(response as unknown as ServerResponse, "delta", {}, controller.signal, 10), RequestAbortedError);
    if (reason === "abort") controller.abort();
    else if (reason !== "timeout") socket.emit(reason);
    await writing;
    assert.equal(socket.eventNames().length, 0);
    assert.equal(getEventListeners(controller.signal, "abort").length, 0);
  }
});

test("streaming sends provider-neutral SSE deltas and done with request/rate headers", async (t) => {
  const { app, limiter } = chatApp({
    async chat() { throw new Error("unused"); },
    async stream(data, emit) { assert.deepEqual(data, chatPayload); await emit("Hello\n"); await emit("world"); },
  });
  t.after(() => app.close());
  const response = await app.inject({ method: "POST", url: "/v1/chat/stream", headers: chatHeaders, payload: chatPayload });
  assert.equal(response.statusCode, 200);
  assert.match(response.headers["content-type"]!, /text\/event-stream/);
  assert.equal(response.headers["x-request-id"], "stream-request");
  assert.equal(response.headers["x-ratelimit-remaining"], "9");
  assert.equal(response.headers["x-accel-buffering"], "no");
  assert.equal(response.payload, 'event: delta\ndata: {"content":"Hello\\n","requestId":"stream-request"}\n\n'
    + 'event: delta\ndata: {"content":"world","requestId":"stream-request"}\n\n'
    + 'event: done\ndata: {"requestId":"stream-request"}\n\n');
  assert.equal(limiter.snapshot().active, 0);
});

test("stream errors are HTTP errors before output and sanitized SSE errors afterward", async (t) => {
  for (const partial of [false, true]) {
    const { app, limiter } = chatApp({
      async chat() { throw new Error("unused"); },
      async stream(_data, emit) {
        if (partial) await emit("partial");
        throw new ProviderConnectionError("upstream", new Error("secret credentials"));
      },
    });
    t.after(() => app.close());
    const response = await app.inject({ method: "POST", url: "/v1/chat/stream", headers: chatHeaders, payload: chatPayload });
    assert.equal(response.statusCode, partial ? 200 : 503);
    if (partial) {
      assert.match(response.payload, /event: error/);
      assert.doesNotMatch(response.payload, /event: done/);
    } else assert.equal(response.json().error.code, "PROVIDER_UNAVAILABLE");
    assert.doesNotMatch(response.payload, /secret credentials/);
    assert.equal(limiter.snapshot().active, 0);
  }
});

test("streaming checks auth, validation, rate limits, and shared execution capacity before starting SSE", async (t) => {
  let calls = 0;
  const { app, limiter, limitRequests } = chatApp({
    async chat() { throw new Error("unused"); }, async stream() { calls += 1; },
  }, 0);
  t.after(() => app.close());
  const inject = (headers = chatHeaders, payload: unknown = chatPayload) =>
    app.inject({ method: "POST", url: "/v1/chat/stream", headers, payload: payload as object });
  assert.equal((await inject({ ...chatHeaders, authorization: "invalid" })).statusCode, 401);
  assert.equal((await inject(chatHeaders, {})).statusCode, 400);
  const occupied = await limiter.acquire();
  assert.equal((await inject()).statusCode, 503);
  occupied.release();
  limitRequests();
  assert.equal((await inject()).statusCode, 429);
  assert.equal(calls, 0);
});

test("a valid empty stream still ends with done", async (t) => {
  const { app } = chatApp({ async chat() { throw new Error("unused"); }, async stream() {} });
  t.after(() => app.close());
  const response = await app.inject({ method: "POST", url: "/v1/chat/stream", headers: chatHeaders, payload: chatPayload });
  assert.equal(response.payload, 'event: done\ndata: {"requestId":"stream-request"}\n\n');
});
