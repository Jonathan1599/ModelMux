import { setTimeout as delay } from "node:timers/promises";
import type { Logger } from "pino";
import { AppError, JobQueueUnavailableError } from "../errors";
import { JobProcessingError } from "./job-error";
import type { JobProcessor, JobQueue, JobReceipt, JobStatus, JobSubmission, JobWorker } from "./job-queue";
import type { JobEnvelope, PostgresJobStore } from "./postgres-job-store";
import type { RabbitBroker } from "./rabbitmq-broker";

export class RabbitMqJobQueue<TData, TResult> implements JobQueue<TData, TResult> {
  public constructor(private readonly store: Pick<PostgresJobStore<TData, TResult>, "enqueue" | "getJob">) {}

  public async enqueue(submission: JobSubmission<TData>): Promise<JobReceipt> {
    try {
      // The job and its pending-publication flag are committed together.
      // The worker dispatcher handles confirmed delivery to RabbitMQ.
      return await this.store.enqueue(submission);
    } catch (error) {
      if (error instanceof AppError) throw error;
      throw new JobQueueUnavailableError(error);
    }
  }

  public async getJob(id: string, ownerId: string): Promise<JobStatus<TResult>> {
    try {
      return await this.store.getJob(id, ownerId);
    } catch (error) {
      if (error instanceof AppError) throw error;
      throw new JobQueueUnavailableError(error);
    }
  }
}

type WorkerStore<TData, TResult> = Pick<PostgresJobStore<TData, TResult>,
  "dispatchOne" | "claim" | "complete" | "fail" | "maintain"
>;

export class RabbitMqJobWorker<TData, TResult> implements JobWorker {
  private readonly stop = new AbortController();
  private running: Promise<void> | undefined;
  private closing: Promise<void> | undefined;

  public constructor(
    private readonly broker: RabbitBroker,
    private readonly store: WorkerStore<TData, TResult>,
    private readonly processor: JobProcessor<TData, TResult>,
    private readonly logger: Logger,
  ) {}

  public run(): Promise<void> {
    this.running ??= this.loop();
    return this.running;
  }

  public close(): Promise<void> {
    this.closing ??= this.drain();
    return this.closing;
  }

  private async loop(): Promise<void> {
    await this.broker.connect();
    if (this.stop.signal.aborted) return;
    await this.broker.consume((message) => this.execute(message));
    this.logger.info("RabbitMQ inference worker started");
    while (!this.stop.signal.aborted) {
      if (this.broker.failure) throw this.broker.failure;
      await this.store.maintain();
      // Multiple dispatchers safely cooperate using SKIP LOCKED. Limit each
      // batch so maintenance and shutdown are checked during sustained load.
      for (let count = 0; count < 100 && !this.stop.signal.aborted; count += 1) {
        const sent = await this.store.dispatchOne((message) => this.broker.publish(message));
        if (!sent) break;
      }
      await delay(1_000, undefined, { signal: this.stop.signal }).catch((error: unknown) => {
        if (!this.stop.signal.aborted) throw error;
      });
    }
  }

  private async execute(message: JobEnvelope): Promise<"ack" | "dead-letter"> {
    const job = await this.store.claim(message);
    if (!job) return "ack"; // Already active, terminal, missing, or an obsolete attempt.
    let response: TResult;
    try {
      response = await this.processor(job);
    } catch (error) {
      const failure = error instanceof JobProcessingError
        ? error : new JobProcessingError("JOB_FAILED", false);
      const status = await this.store.fail(message, failure);
      this.logger.warn({
        reqId: job.requestId, jobId: job.id, attempt: job.attempt,
        status, code: failure.code,
      }, "Job attempt failed");
      return status === "failed" ? "dead-letter" : "ack";
    }
    // Persist first, acknowledge second. Database failures leave the RabbitMQ
    // delivery unacknowledged; they are not classified as provider failures.
    await this.store.complete(message, response);
    return "ack";
  }

  private async drain(): Promise<void> {
    this.stop.abort();
    try {
      await this.broker.stopConsuming();
      await this.running?.catch(() => undefined);
    } finally {
      await this.broker.close();
    }
  }
}
