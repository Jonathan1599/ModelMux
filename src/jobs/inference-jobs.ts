import type { JobQueue } from "../queue/job-queue";
import type { ChatRequest, ChatResponse } from "../types/chat";

export type InferenceJobs = JobQueue<ChatRequest, ChatResponse>;

export const INFERENCE_QUEUE_NAME = "modelmux-inference";
