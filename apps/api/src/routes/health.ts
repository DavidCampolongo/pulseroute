import type { FastifyPluginAsync } from "fastify";

import { AppError } from "../errors.js";

export const healthRoutes: FastifyPluginAsync = async (app) => {
  app.get(
    "/health",
    {
      schema: {
        response: {
          200: {
            type: "object",
            additionalProperties: false,
            required: ["status", "service", "database"],
            properties: {
              status: {
                type: "string",
                const: "ok",
              },
              service: {
                type: "string",
                const: "pulseroute-api",
              },
              database: {
                type: "string",
                const: "reachable",
              },
            },
          },
          503: {
            type: "object",
            additionalProperties: false,
            required: ["requestId", "code", "message"],
            properties: {
              requestId: {
                type: "string",
              },
              code: {
                type: "string",
                const: "DATABASE_UNAVAILABLE",
              },
              message: {
                type: "string",
                const: "Database is unavailable",
              },
            },
          },
        },
      },
    },
    async () => {
      try {
        await app.db.$queryRaw`SELECT 1`;

        return {
          status: "ok",
          service: "pulseroute-api",
          database: "reachable",
        };
      } catch (error) {
        throw new AppError(
          503,
          "DATABASE_UNAVAILABLE",
          "Database is unavailable",
          { cause: error },
        );
      }
    },
  );
};
