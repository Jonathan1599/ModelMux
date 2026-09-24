import type { FastifyPluginAsync } from "fastify";
import { AuthenticationError } from "../errors";
import type { InferenceJobs } from "../jobs/inference-jobs";
import type { JobReceipt, JobStatus } from "../queue/job-queue";
import type { ChatRequest, ChatResponse } from "../types/chat";
import { chatRequestSchema, chatResponseSchema } from "./chat-schema";

const receiptProperties = {
  id: { type: "string" },
  requestId: { type: "string" },
};

export const jobRoutes: FastifyPluginAsync<{ jobs: InferenceJobs }> = async (
  app,
  { jobs },
) => {
  app.post<{
    Body: ChatRequest;
    Headers: { "idempotency-key": string };
    Reply: JobReceipt;
  }>("/v1/jobs", {
    schema: {
      body: chatRequestSchema,
      headers: {
        type: "object",
        required: ["idempotency-key"],
        properties: {
          "idempotency-key": {
            type: "string",
            minLength: 1,
            maxLength: 128,
            pattern: "^[A-Za-z0-9._-]+$",
          },
        },
      },
      response: {
        202: {
          type: "object",
          required: ["id", "requestId"],
          additionalProperties: false,
          properties: receiptProperties,
        },
      },
    },
  }, async (request, response) => {
    if (!request.apiKey) throw new AuthenticationError();

    const job = await jobs.enqueue({
      ownerId: request.apiKey.id,
      requestId: request.id,
      idempotencyKey: request.headers["idempotency-key"],
      data: request.body,
    });
    request.log.info({ jobId: job.id, jobRequestId: job.requestId }, "Inference job accepted");

    return response.code(202).header("location", `/v1/jobs/${job.id}`).send(job);
  });

  app.get<{ Params: { id: string }; Reply: JobStatus<ChatResponse> }>("/v1/jobs/:id", {
    schema: {
      params: {
        type: "object",
        required: ["id"],
        properties: { id: { type: "string", minLength: 1, maxLength: 128, pattern: "^[A-Za-z0-9_-]+$" } },
      },
      response: {
        200: {
          type: "object",
          additionalProperties: false,
          required: ["id", "requestId", "status", "attemptsMade"],
          properties: {
            ...receiptProperties,
            status: { type: "string", enum: ["queued", "active", "retrying", "completed", "failed"] },
            attemptsMade: { type: "integer" },
            response: chatResponseSchema,
            error: {
              type: "object",
              additionalProperties: false,
              required: ["code", "message"],
              properties: { code: { type: "string" }, message: { type: "string" } },
            },
          },
        },
      },
    },
  }, async (request, response) => {
    if (!request.apiKey) throw new AuthenticationError();
    void response.header("cache-control", "no-store");
    return jobs.getJob(request.params.id, request.apiKey.id);
  });
};
