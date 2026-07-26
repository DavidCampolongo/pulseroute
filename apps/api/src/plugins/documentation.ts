import swagger from "@fastify/swagger";
import swaggerUi from "@fastify/swagger-ui";
import type { FastifyPluginAsync } from "fastify";
import fastifyPlugin from "fastify-plugin";

export const documentationPlugin: FastifyPluginAsync = fastifyPlugin(
  async (app) => {
    await app.register(swagger, {
      openapi: {
        info: {
          title: "PulseRoute API",
          description: "PulseRoute backend HTTP API",
          version: "0.1.0",
        },
      },
    });

    await app.register(swaggerUi, {
      routePrefix: "/docs",
    });
  },
  {
    name: "documentation",
  },
);
