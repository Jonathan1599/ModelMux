import assert from "node:assert/strict";
import test from "node:test";
import { loadConfig } from "../src/config";

test("async job configuration has defaults and accepts overrides", () => {
  const defaults = loadConfig({});
  assert.equal(defaults.jobConcurrency, 2);
  assert.equal(defaults.jobMaxAttempts, 3);
  assert.equal(defaults.jobBackoffMs, 1_000);
  assert.equal(defaults.rabbitmqUrl, "amqp://modelmux:modelmux@localhost:5672");
  const config = loadConfig({ JOB_CONCURRENCY: "1", JOB_MAX_ATTEMPTS: "5", JOB_BACKOFF_MS: "250" });
  assert.equal(config.jobConcurrency, 1);
  assert.equal(config.jobMaxAttempts, 5);
  assert.equal(config.jobBackoffMs, 250);
});

test("RabbitMQ configuration accepts AMQP/TLS URLs and rejects other protocols", () => {
  assert.equal(loadConfig({ RABBITMQ_URL: "amqps://localhost:5671" }).rabbitmqUrl, "amqps://localhost:5671");
  assert.throws(() => loadConfig({ RABBITMQ_URL: "http://localhost" }), /RABBITMQ_URL/);
});

test("async job configuration rejects invalid concurrency, attempts, and backoff", () => {
  for (const name of ["JOB_CONCURRENCY", "JOB_MAX_ATTEMPTS", "JOB_BACKOFF_MS"]) {
    for (const value of ["0", "-1", "1.5", "bad", "Infinity", "2147483648"]) {
      assert.throws(() => loadConfig({ [name]: value }), new RegExp(name));
    }
  }
});
