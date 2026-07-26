import Fastify, { type FastifyInstance } from "fastify";

import type { AppConfig } from "./config.js";
import { registerErrorHandler } from "./error-handler.js";
import { databasePlugin } from "./plugins/database.js";
import { documentationPlugin } from "./plugins/documentation.js";
import { healthRoutes } from "./routes/health.js";

export function buildApp(config: AppConfig): FastifyInstance {
  const app = Fastify({
    logger: {
      level: config.logLevel,
    },
  });

  registerErrorHandler(app);

  app.register(databasePlugin, {
    databaseUrl: config.databaseUrl,
  });

  app.register(documentationPlugin);

  app.register(healthRoutes);

  return app;
}
