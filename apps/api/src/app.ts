import Fastify, { type FastifyInstance } from "fastify";

import type { AppConfig } from "./config.js";

export function buildApp(config: AppConfig): FastifyInstance {
  const app = Fastify({
    logger: {
      level: config.logLevel,
    },
  });

  app.get(
    "/health",
    {
      schema: {
        response: {
          200: {
            type: "object",
            additionalProperties: false,
            required: ["status", "service"],
            properties: {
              status: {
                type: "string",
                const: "ok",
              },
              service: {
                type: "string",
                const: "pulseroute-api",
              },
            },
          },
        },
      },
    },
    async () => {
      return {
        status: "ok",
        service: "pulseroute-api",
      };
    },
  );

  return app;
}
