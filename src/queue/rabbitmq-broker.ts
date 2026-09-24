import { connect, type Channel, type ChannelModel, type ConfirmChannel, type ConsumeMessage, type Message } from "amqplib";
import type { JobEnvelope } from "./postgres-job-store";

export interface RabbitBroker {
  readonly failure: Error | undefined;
  connect(): Promise<void>;
  publish(message: JobEnvelope): Promise<void>;
  consume(handler: (message: JobEnvelope) => Promise<"ack" | "dead-letter">): Promise<void>;
  stopConsuming(): Promise<void>;
  close(): Promise<void>;
}

export interface RabbitMqOptions {
  url: string;
  name: string;
  concurrency: number;
}

export class RabbitMqBroker implements RabbitBroker {
  public failure: Error | undefined;
  private connection: ChannelModel | undefined;
  private publisher: ConfirmChannel | undefined;
  private consumer: Channel | undefined;
  private consumerTag: string | undefined;
  private readonly active = new Set<Promise<void>>();
  private draining = false;
  private closing = false;

  public constructor(
    private readonly options: RabbitMqOptions,
    private readonly connectClient: typeof connect = connect,
  ) {}

  public async connect(): Promise<void> {
    const url = new URL(this.options.url);
    if (!url.searchParams.has("heartbeat")) url.searchParams.set("heartbeat", "10");
    this.connection = await this.connectClient(url.toString(), { timeout: 5_000 });
    this.observe(this.connection);
    this.publisher = await this.connection.createConfirmChannel();
    this.observe(this.publisher);
    this.consumer = await this.connection.createChannel();
    this.observe(this.consumer);

    await this.publisher.assertQueue(`${this.options.name}.failed`, {
      durable: true,
      arguments: { "x-queue-type": "quorum", "x-message-ttl": 86_400_000 },
    });
    await this.publisher.assertQueue(this.options.name, {
      durable: true,
      arguments: {
        "x-queue-type": "quorum",
        "x-single-active-consumer": true,
        "x-dead-letter-exchange": "",
        "x-dead-letter-routing-key": `${this.options.name}.failed`,
        "x-dead-letter-strategy": "at-least-once",
        "x-overflow": "reject-publish",
        // Application attempts live in Postgres. Infrastructure redeliveries
        // must not silently exhaust RabbitMQ's independent delivery counter.
        "x-delivery-limit": -1,
      },
    });
    await this.consumer.prefetch(this.options.concurrency);
  }

  public async publish(message: JobEnvelope): Promise<void> {
    const channel = this.publisher;
    if (!channel || this.failure) throw this.failure ?? new Error("RabbitMQ is not connected");
    const messageId = `${message.id}/${message.attempt}`;

    // One publication at a time, bounded by a confirm timeout. A positive
    // publisher confirm alone is insufficient when mandatory routing returns
    // an unroutable message; in that case leave the outbox row pending.
    await new Promise<void>((resolve, reject) => {
      const finish = (error?: unknown) => {
        clearTimeout(timer);
        channel.off("return", onReturn);
        channel.off("close", onClose);
        if (error) reject(error);
        else resolve();
      };
      const onReturn = (returned: Message) => {
        if (returned.properties.messageId === messageId) {
          finish(new Error("RabbitMQ could not route the job"));
        }
      };
      const onClose = () => finish(new Error("RabbitMQ channel closed before confirmation"));
      const timer = setTimeout(() => finish(new Error("RabbitMQ publish confirmation timed out")), 5_000);
      channel.on("return", onReturn);
      channel.on("close", onClose);
      try {
        channel.sendToQueue(this.options.name, Buffer.from(JSON.stringify(message)), {
          persistent: true,
          mandatory: true,
          contentType: "application/json",
          messageId,
        }, (error: unknown) => finish(error));
      } catch (error) {
        finish(error);
      }
    });
  }

  public async consume(handler: (message: JobEnvelope) => Promise<"ack" | "dead-letter">): Promise<void> {
    const channel = this.consumer;
    if (!channel) throw new Error("RabbitMQ is not connected");
    const { consumerTag } = await channel.consume(this.options.name, (message) => {
      if (!message) {
        if (!this.closing) this.failure = new Error("RabbitMQ cancelled the consumer");
        return;
      }
      // Keep new deliveries unacknowledged while draining. Cancelling the
      // single active consumer before its running calls finish would let a
      // standby start additional calls and exceed the normal concurrency cap.
      if (this.draining) return;
      const task = this.handle(channel, message, handler).catch((error: unknown) => {
        // Leave the delivery unacknowledged. Closing the failed worker's
        // connection requeues it, rather than spinning on an unavailable DB.
        this.failure = error instanceof Error ? error : new Error("Job delivery failed");
      });
      this.active.add(task);
      void task.then(() => this.active.delete(task));
    }, { noAck: false });
    this.consumerTag = consumerTag;
  }

  public async stopConsuming(): Promise<void> {
    this.draining = true;
    await Promise.all(this.active);
    if (!this.consumerTag || !this.consumer) return;
    const tag = this.consumerTag;
    this.consumerTag = undefined;
    try {
      await this.consumer.cancel(tag);
    } catch (error) {
      if (!this.failure) throw error;
    }
  }

  public async close(): Promise<void> {
    this.closing = true;
    try {
      await this.stopConsuming();
      await Promise.all(this.active);
    } finally {
      try { await this.connection?.close(); } catch (error) {
        if (!this.failure) throw error;
      }
    }
  }

  private async handle(
    channel: Channel,
    delivery: ConsumeMessage,
    handler: (message: JobEnvelope) => Promise<"ack" | "dead-letter">,
  ): Promise<void> {
    let message: unknown;
    try { message = JSON.parse(delivery.content.toString("utf8")); } catch { /* reject below */ }
    if (!isJobEnvelope(message)) {
      channel.reject(delivery, false);
      return;
    }
    const action = await handler(message);
    if (action === "dead-letter") channel.reject(delivery, false);
    else channel.ack(delivery);
  }

  private observe(emitter: ChannelModel | Channel): void {
    emitter.on("error", (error: Error) => { if (!this.closing) this.failure = error; });
    emitter.on("close", () => {
      if (!this.closing) this.failure ??= new Error("RabbitMQ connection or channel closed");
    });
  }
}

function isJobEnvelope(value: unknown): value is JobEnvelope {
  if (!value || typeof value !== "object") return false;
  const message = value as Partial<JobEnvelope>;
  return typeof message.id === "string" && /^[A-Za-z0-9_-]{1,128}$/.test(message.id)
    && Number.isSafeInteger(message.attempt) && (message.attempt ?? 0) > 0
    && (message.attempt ?? 0) <= 2_147_483_647;
}
