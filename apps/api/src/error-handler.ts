import type { FastifyError, FastifyInstance } from "fastify";

import { AppError } from "./errors.js";

export function registerErrorHandler(app: FastifyInstance): void {
  app.setErrorHandler((error: FastifyError, request, reply) => {
    if (error.validation) {
      return reply.status(400).send({
        requestId: request.id,
        code: "VALIDATION_ERROR",
        message: "Request validation failed",
        details: error.validation,
      });
    }

    if (error instanceof AppError) {
      request.log.warn({ err: error }, error.message);

      return reply.status(error.statusCode).send({
        requestId: request.id,
        code: error.code,
        message: error.message,
      });
    }

    request.log.error({ err: error }, "Unexpected request error");

    return reply.status(500).send({
      requestId: request.id,
      code: "INTERNAL_ERROR",
      message: "An unexpected error occurred",
    });
  });
}
