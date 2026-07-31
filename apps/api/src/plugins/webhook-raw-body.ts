import type { FastifyInstance } from "fastify";

declare module "fastify" {
  interface FastifyRequest {
    rawBody: Buffer | null;
  }
}

export function registerWebhookRawBodyParser(app: FastifyInstance): void {
  app.decorateRequest("rawBody", null);

  app.removeContentTypeParser("application/json");

  app.addContentTypeParser(
    "application/json",
    {
      parseAs: "buffer",
    },
    (request, body, done) => {
      if (!Buffer.isBuffer(body)) {
        done(new Error("Expected webhook body to be parsed as a Buffer"));
        return;
      }

      request.rawBody = body;

      done(null, body);
    },
  );
}
