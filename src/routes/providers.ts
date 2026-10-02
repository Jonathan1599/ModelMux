import type { FastifyPluginAsync } from "fastify";
import type { ProviderHealth } from "../providers/router";

export const providerRoutes: FastifyPluginAsync<{
  snapshot: () => ProviderHealth[];
}> = async (app, { snapshot }) => {
  app.get("/v1/providers", {
    schema: {
      response: {
        200: {
          type: "object",
          additionalProperties: false,
          required: ["scope", "providers"],
          properties: {
            scope: { type: "string", enum: ["process"] },
            providers: {
              type: "array",
              items: {
                type: "object",
                additionalProperties: false,
                required: ["name", "state", "consecutiveFailures", "retryAfterMs"],
                properties: {
                  name: { type: "string" },
                  state: { type: "string", enum: ["closed", "open", "half-open"] },
                  consecutiveFailures: { type: "integer", minimum: 0 },
                  retryAfterMs: { type: "number", minimum: 0 },
                },
              },
            },
          },
        },
      },
    },
  }, async (_request, response) => {
    void response.header("cache-control", "no-store");
    return { scope: "process", providers: snapshot() };
  });
};
