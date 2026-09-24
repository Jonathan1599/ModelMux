// Shared by synchronous chat and asynchronous job submission.
export const chatRequestSchema = {
  type: "object",
  additionalProperties: false,
  required: ["model", "messages"],
  properties: {
    model: { type: "string", minLength: 1 },
    messages: {
      type: "array",
      minItems: 1,
      items: {
        type: "object",
        additionalProperties: false,
        required: ["role", "content"],
        properties: {
          role: { type: "string", enum: ["system", "user", "assistant"] },
          content: { type: "string", minLength: 1 },
        },
      },
    },
  },
};

export const chatResponseSchema = {
  type: "object",
  additionalProperties: false,
  required: ["message"],
  properties: {
    message: {
      type: "object",
      additionalProperties: false,
      required: ["role", "content"],
      properties: {
        role: { type: "string", enum: ["assistant"] },
        content: { type: "string" },
      },
    },
  },
};
