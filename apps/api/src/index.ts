import { config as loadEnvironmentFile } from "dotenv";
import type { FastifyInstance } from "fastify";

import { buildApp } from "./app.js";
import { parseConfig } from "./config.js";

loadEnvironmentFile({
  path: "../../.env",
  quiet: true,
});

let app: FastifyInstance | undefined;
let isShuttingDown = false;

async function start(): Promise<void> {
  try {
    const config = parseConfig(process.env);

    app = buildApp(config);

    await app.listen({
      host: config.host,
      port: config.port,
    });
  } catch (error) {
    if (app) {
      app.log.error({ err: error }, "API startup failed");

      await app.close();
    } else {
      const message =
        error instanceof Error ? error.message : "Unknown API startup failure";

      console.error(message);
    }

    process.exitCode = 1;
  }
}

async function shutdown(signal: string): Promise<void> {
  if (!app || isShuttingDown) {
    return;
  }

  isShuttingDown = true;
  app.log.info({ signal }, "API shutdown started");

  try {
    await app.close();
  } catch (error) {
    app.log.error({ err: error }, "API shutdown failed");
    process.exitCode = 1;
  }
}

process.on("SIGINT", () => {
  void shutdown("SIGINT");
});

process.on("SIGTERM", () => {
  void shutdown("SIGTERM");
});

await start();
