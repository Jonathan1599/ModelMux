export interface JobSubmission<TData> {
  ownerId: string;
  requestId: string;
  idempotencyKey: string;
  data: TData;
}

export interface JobReceipt {
  id: string;
  requestId: string;
}

export interface JobStatus<TResult> extends JobReceipt {
  status: "queued" | "active" | "retrying" | "completed" | "failed";
  attemptsMade: number;
  response?: TResult;
  error?: { code: string; message: string };
}

// Adapters must atomically deduplicate by owner + idempotency key, reject
// conflicting payloads, and hide other owners' jobs as not found.
// Data/results must be JSON-serializable. Deduplication lasts while the job is
// retained; delivery may repeat, so processors must tolerate duplicate attempts.
export interface JobQueue<TData, TResult> {
  enqueue(submission: JobSubmission<TData>): Promise<JobReceipt>;
  getJob(id: string, ownerId: string): Promise<JobStatus<TResult>>;
}

export interface ProcessingJob<TData> extends JobReceipt {
  ownerId: string;
  data: TData;
  attempt: number;
}

export type JobProcessor<TData, TResult> = (
  job: ProcessingJob<TData>,
) => Promise<TResult>;

export interface JobWorker {
  // Runs until close() stops admissions and drains active work.
  run(): Promise<void>;
  close(): Promise<void>;
}
