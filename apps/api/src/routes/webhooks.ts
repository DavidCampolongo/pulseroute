import type { FastifyPluginAsync } from "fastify";

import { AppError } from "../errors.js";
import { registerWebhookRawBodyParser } from "../plugins/webhook-raw-body.js";

export const webhookRoutes: FastifyPluginAsync = async (app) => {
  registerWebhookRawBodyParser(app);

  app.post("/service-requests", async (request) => {
    if (!Buffer.isBuffer(request.body) || !Buffer.isBuffer(request.rawBody)) {
      throw new Error("Webhook raw body was not captured as a Buffer");
    }

    throw new AppError(
      501,
      "WEBHOOK_NOT_IMPLEMENTED",
      "Webhook ingestion is not implemented yet",
    );
  });
};
