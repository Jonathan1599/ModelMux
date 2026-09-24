import { createHash, randomUUID } from "node:crypto";
import type { Pool } from "pg";
import { IdempotencyConflictError, JobNotFoundError } from "../errors";
import { JobProcessingError, publicJobError } from "./job-error";
import type { JobReceipt, JobStatus, JobSubmission, ProcessingJob } from "./job-queue";

export interface JobEnvelope {
  id: string;
  attempt: number;
}

interface JobRow<TData, TResult> {
  id: string;
  owner_id: string;
  request_id: string;
  payload_hash: string;
  data: TData;
  status: JobStatus<TResult>["status"];
  attempts_made: number;
  response: TResult;
  error_code: string | null;
}

export interface JobStoreOptions {
  name: string;
  maxAttempts: number;
  backoffMs: number;
  leaseMs: number;
}

// Postgres is the source of truth for idempotency, results, and pending
// publication. RabbitMQ only carries a job ID and its attempt number.
export class PostgresJobStore<TData, TResult> {
  public constructor(
    private readonly pool: Pick<Pool, "query" | "connect">,
    private readonly options: JobStoreOptions,
  ) {}

  public async enqueue(submission: JobSubmission<TData>): Promise<JobReceipt> {
    const keyHash = hash(submission.idempotencyKey);
    const payloadHash = hash(submission.data);
    const values = [this.options.name, submission.ownerId, keyHash];
    const inserted = await this.pool.query<JobRow<TData, TResult>>(
      `INSERT INTO inference_jobs
         (queue_name, owner_id, idempotency_hash, id, request_id, payload_hash,
          data, max_attempts, backoff_ms)
       VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9)
       ON CONFLICT (queue_name, owner_id, idempotency_hash) DO NOTHING
       RETURNING *`,
      [...values, `job-${randomUUID()}`, submission.requestId, payloadHash,
        JSON.stringify(submission.data), this.options.maxAttempts, this.options.backoffMs],
    );
    const row = inserted.rows[0] ?? (await this.pool.query<JobRow<TData, TResult>>(
      `SELECT * FROM inference_jobs
       WHERE queue_name = $1 AND owner_id = $2 AND idempotency_hash = $3`,
      values,
    )).rows[0];
    if (!row) throw new Error("Job disappeared during submission; retry the request");
    if (row.payload_hash !== payloadHash) throw new IdempotencyConflictError();
    return { id: row.id, requestId: row.request_id };
  }

  public async getJob(id: string, ownerId: string): Promise<JobStatus<TResult>> {
    const { rows: [row] } = await this.pool.query<JobRow<TData, TResult>>(
      `SELECT * FROM inference_jobs WHERE id = $1 AND owner_id = $2 AND queue_name = $3`,
      [id, ownerId, this.options.name],
    );
    if (!row) throw new JobNotFoundError();
    const status: JobStatus<TResult> = {
      id: row.id,
      requestId: row.request_id,
      status: row.status,
      attemptsMade: row.attempts_made,
    };
    if (row.status === "completed") status.response = row.response;
    if (row.status === "failed") status.error = publicJobError(row.error_code ?? "JOB_FAILED");
    return status;
  }

  public async dispatchOne(publish: (message: JobEnvelope) => Promise<void>): Promise<boolean> {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const { rows: [row] } = await client.query<{ id: string; attempts_made: number }>(
        `SELECT id, attempts_made FROM inference_jobs
         WHERE queue_name = $1 AND dispatch_pending = TRUE
           AND status IN ('queued', 'retrying') AND available_at <= NOW()
         ORDER BY available_at, id LIMIT 1 FOR UPDATE SKIP LOCKED`,
        [this.options.name],
      );
      if (row) {
        // Keep the row locked until the broker confirms publication. A crash
        // before COMMIT leaves it pending; duplicate deliveries are fenced by
        // the atomic attempt claim below.
        await publish({ id: row.id, attempt: row.attempts_made + 1 });
        await client.query("UPDATE inference_jobs SET dispatch_pending = FALSE WHERE id = $1", [row.id]);
      }
      await client.query("COMMIT");
      return Boolean(row);
    } catch (error) {
      await client.query("ROLLBACK").catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  }

  public async claim(message: JobEnvelope): Promise<ProcessingJob<TData> | null> {
    const { rows: [row] } = await this.pool.query<JobRow<TData, TResult>>(
      `UPDATE inference_jobs
       SET status = 'active', attempts_made = $2,
           lease_until = NOW() + $4 * INTERVAL '1 millisecond'
       WHERE id = $1 AND queue_name = $3 AND status IN ('queued', 'retrying')
         AND attempts_made = $2 - 1 AND attempts_made < max_attempts
         AND available_at <= NOW()
       RETURNING *`,
      [message.id, message.attempt, this.options.name, this.options.leaseMs],
    );
    if (!row) return null;
    return {
      id: row.id, ownerId: row.owner_id, requestId: row.request_id,
      data: row.data, attempt: row.attempts_made,
    };
  }

  public async complete(message: JobEnvelope, response: TResult): Promise<void> {
    await this.pool.query(
      `UPDATE inference_jobs
       SET status = 'completed', response = $4::jsonb, error_code = NULL,
           finished_at = NOW(), lease_until = NULL, dispatch_pending = FALSE
       WHERE id = $1 AND attempts_made = $2 AND queue_name = $3 AND status = 'active'`,
      [message.id, message.attempt, this.options.name, JSON.stringify(response)],
    );
  }

  public async fail(message: JobEnvelope, error: JobProcessingError): Promise<"retrying" | "failed" | null> {
    const { rows: [row] } = await this.pool.query<{ status: "retrying" | "failed" }>(
      `UPDATE inference_jobs SET
         status = CASE WHEN $4 AND attempts_made < max_attempts THEN 'retrying' ELSE 'failed' END,
         dispatch_pending = ($4 AND attempts_made < max_attempts),
         available_at = NOW() + LEAST(3600000,
           backoff_ms::double precision * POWER(2, LEAST(attempts_made - 1, 30))) * INTERVAL '1 millisecond',
         error_code = $5, lease_until = NULL,
         finished_at = CASE WHEN $4 AND attempts_made < max_attempts THEN NULL ELSE NOW() END
       WHERE id = $1 AND attempts_made = $2 AND queue_name = $3 AND status = 'active'
       RETURNING status`,
      [message.id, message.attempt, this.options.name, error.retryable, error.code],
    );
    return row?.status ?? null;
  }

  public async maintain(): Promise<void> {
    // A crashed worker may have left an active row after its delivery was
    // requeued. Expired attempts are rescheduled; old workers cannot overwrite
    // a newer attempt's state because updates check the attempt number.
    await this.pool.query(
      `UPDATE inference_jobs SET
         status = CASE WHEN attempts_made < max_attempts THEN 'queued' ELSE 'failed' END,
         dispatch_pending = (attempts_made < max_attempts), available_at = NOW(),
         error_code = 'JOB_FAILED', lease_until = NULL,
         finished_at = CASE WHEN attempts_made < max_attempts THEN NULL ELSE NOW() END
       WHERE queue_name = $1 AND status = 'active' AND lease_until < NOW()`,
      [this.options.name],
    );
    await this.pool.query(
      `DELETE FROM inference_jobs
       WHERE queue_name = $1 AND finished_at < NOW() - INTERVAL '24 hours'`,
      [this.options.name],
    );
  }
}

function hash(value: unknown): string {
  const canonical = JSON.stringify(value, (_key, item: unknown) => {
    if (item !== null && typeof item === "object" && !Array.isArray(item)) {
      return Object.fromEntries(
        Object.entries(item).sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0),
      );
    }
    return item;
  });
  return createHash("sha256").update(canonical).digest("hex");
}
