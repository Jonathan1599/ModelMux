import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import test from "node:test";
import type { ChannelModel, ConsumeMessage, Options } from "amqplib";
import { RabbitMqBroker } from "../src/queue/rabbitmq-broker";

function fakeConnection() {
  const declarations: Array<{ name: string; options: Options.AssertQueue }> = [];
  const events: string[] = [];
  let deliver!: (message: ConsumeMessage | null) => void;
  let confirm!: (error: unknown) => void;
  let publication: { options: Options.Publish; content: Buffer } | undefined;
  const publisher = Object.assign(new EventEmitter(), {
    async assertQueue(name: string, options: Options.AssertQueue) { declarations.push({ name, options }); },
    sendToQueue(_name: string, content: Buffer, options: Options.Publish, callback: typeof confirm) {
      publication = { content, options }; confirm = callback; return true;
    },
  });
  const consumer = Object.assign(new EventEmitter(), {
    async prefetch(count: number) { events.push(`prefetch:${count}`); },
    async consume(_name: string, handler: typeof deliver, options: Options.Consume) {
      assert.equal(options.noAck, false);
      deliver = handler;
      return { consumerTag: "consumer-1" };
    },
    async cancel() { events.push("cancel"); },
    ack() { events.push("ack"); },
    reject(_message: ConsumeMessage, requeue: boolean) { events.push(`reject:${requeue}`); },
  });
  const connection = Object.assign(new EventEmitter(), {
    async createConfirmChannel() { return publisher; },
    async createChannel() { return consumer; },
    async close() { events.push("close"); },
  });
  const broker = new RabbitMqBroker({ name: "test", url: "amqp://localhost", concurrency: 2 },
    async () => connection as unknown as ChannelModel);
  return {
    broker, publisher, declarations, events,
    get publication() { return publication; },
    confirm(error: unknown = null) { confirm(error); },
    deliver(content: string) { deliver({ content: Buffer.from(content) } as ConsumeMessage); },
  };
}

test("RabbitMQ topology uses durable quorum queues, single active consumer, and bounded prefetch", async () => {
  const fixture = fakeConnection();
  await fixture.broker.connect();
  const main = fixture.declarations.find(({ name }) => name === "test")!;
  assert.equal(main.options.durable, true);
  assert.equal(main.options.arguments["x-queue-type"], "quorum");
  assert.equal(main.options.arguments["x-single-active-consumer"], true);
  assert.equal(main.options.arguments["x-dead-letter-routing-key"], "test.failed");
  assert.deepEqual(fixture.events, ["prefetch:2"]);
  await fixture.broker.close();
});

test("publishing waits for a broker confirm and requests persistent mandatory routing", async () => {
  const fixture = fakeConnection();
  await fixture.broker.connect();
  let resolved = false;
  const sending = fixture.broker.publish({ id: "job-1", attempt: 1 }).then(() => { resolved = true; });
  await Promise.resolve();
  assert.equal(resolved, false);
  assert.equal(fixture.publication?.options.persistent, true);
  assert.equal(fixture.publication?.options.mandatory, true);
  assert.deepEqual(JSON.parse(fixture.publication!.content.toString()), { id: "job-1", attempt: 1 });
  fixture.confirm();
  await sending;
  assert.equal(resolved, true);
  await fixture.broker.close();
});

test("unroutable publications fail even if RabbitMQ subsequently confirms them", async () => {
  const fixture = fakeConnection();
  await fixture.broker.connect();
  const sending = fixture.broker.publish({ id: "job-1", attempt: 1 });
  const rejection = assert.rejects(sending, /could not route/);
  fixture.publisher.emit("return", { properties: { messageId: "job-1/1" } });
  fixture.confirm();
  await rejection;
  assert.equal(fixture.publisher.listenerCount("return"), 0);
  await fixture.broker.close();
});

test("consumer acknowledgement waits for processing and shutdown drains active deliveries", async () => {
  const fixture = fakeConnection();
  await fixture.broker.connect();
  let finish!: () => void;
  let calls = 0;
  const processing = new Promise<void>((resolve) => { finish = resolve; });
  await fixture.broker.consume(async () => { calls += 1; await processing; return "ack"; });
  fixture.deliver('{"id":"job-1","attempt":1}');
  const closing = fixture.broker.close();
  fixture.deliver('{"id":"job-2","attempt":1}');
  await Promise.resolve();
  assert.ok(!fixture.events.includes("ack"));
  assert.ok(!fixture.events.includes("close"));
  assert.ok(!fixture.events.includes("cancel"));
  assert.equal(calls, 1);
  finish();
  await closing;
  assert.deepEqual(fixture.events, ["prefetch:2", "ack", "cancel", "close"]);
});

test("malformed messages and terminal failures are dead-lettered", async () => {
  const fixture = fakeConnection();
  await fixture.broker.connect();
  let calls = 0;
  await fixture.broker.consume(async () => { calls += 1; return "dead-letter"; });
  fixture.deliver("invalid json");
  fixture.deliver('{"id":"job-1","attempt":0}');
  fixture.deliver('{"id":"job-1","attempt":1}');
  await fixture.broker.close();
  assert.equal(calls, 1);
  assert.equal(fixture.events.filter((event) => event === "reject:false").length, 3);
});

test("infrastructure failures leave deliveries unacknowledged for connection-close redelivery", async () => {
  const fixture = fakeConnection();
  await fixture.broker.connect();
  const error = new Error("database down");
  await fixture.broker.consume(async () => { throw error; });
  fixture.deliver('{"id":"job-1","attempt":1}');
  await fixture.broker.close();
  assert.equal(fixture.broker.failure, error);
  assert.ok(!fixture.events.includes("ack"));
  assert.ok(!fixture.events.some((event) => event.startsWith("reject")));
});
