import { isUtf8 } from "node:buffer";

import type { FastifyPluginAsync, FastifyRequest } from "fastify";

import { AppError } from "../errors.js";
import { registerWebhookRawBodyParser } from "../plugins/webhook-raw-body.js";
import {
  WEBHOOK_SIGNATURE_HEADER,
  WEBHOOK_TIMESTAMP_HEADER,
  serviceRequestWebhookSchema,
} from "../webhooks/contract.js";
import { verifyWebhookSignature } from "../webhooks/signature.js";

type WebhookRouteOptions = {
  webhookSecret: string;
  webhookToleranceSeconds: number;
};

const provider = "pulseroute";

function readHeader(
  request: FastifyRequest,
  headerName: string,
): string | undefined {
  const value = request.headers[headerName];

  return typeof value === "string" ? value : undefined;
}

function parseAuthenticatedJson(rawBody: Buffer): unknown {
  if (!isUtf8(rawBody)) {
    throw new AppError(
      400,
      "WEBHOOK_PAYLOAD_INVALID",
      "Webhook payload is invalid",
    );
  }

  try {
    return JSON.parse(rawBody.toString("utf8")) as unknown;
  } catch {
    throw new AppError(
      400,
      "WEBHOOK_PAYLOAD_INVALID",
      "Webhook payload is invalid",
    );
  }
}

export const webhookRoutes: FastifyPluginAsync<WebhookRouteOptions> = async (
  app,
  options,
) => {
  registerWebhookRawBodyParser(app);

  app.post("/service-requests", async (request) => {
    if (!Buffer.isBuffer(request.body) || !Buffer.isBuffer(request.rawBody)) {
      throw new AppError(
        415,
        "WEBHOOK_CONTENT_TYPE_UNSUPPORTED",
        "Webhook requests must use application/json",
      );
    }

    const timestamp = readHeader(request, WEBHOOK_TIMESTAMP_HEADER);

    const signature = readHeader(request, WEBHOOK_SIGNATURE_HEADER);

    const verification = verifyWebhookSignature({
      secret: options.webhookSecret,
      timestamp,
      signature,
      rawBody: request.rawBody,
      toleranceSeconds: options.webhookToleranceSeconds,
      nowSeconds: Math.floor(Date.now() / 1000),
    });

    if (!verification.ok) {
      request.log.warn(
        {
          provider,
          webhookVerificationOutcome: "rejected",
          webhookVerificationReason: verification.reason,
        },
        "Webhook authentication failed",
      );

      throw new AppError(
        401,
        "WEBHOOK_AUTHENTICATION_FAILED",
        "Webhook authentication failed",
      );
    }

    let parsedPayload: unknown;

    try {
      parsedPayload = parseAuthenticatedJson(request.rawBody);
    } catch (error) {
      request.log.warn(
        {
          provider,
          webhookVerificationOutcome: "verified",
          ingestionStatus: "invalid_json",
        },
        "Authenticated webhook contained invalid JSON",
      );

      throw error;
    }

    const payloadResult = serviceRequestWebhookSchema.safeParse(parsedPayload);

    if (!payloadResult.success) {
      request.log.warn(
        {
          provider,
          webhookVerificationOutcome: "verified",
          ingestionStatus: "invalid_payload",
        },
        "Authenticated webhook failed payload validation",
      );

      throw new AppError(
        400,
        "WEBHOOK_PAYLOAD_INVALID",
        "Webhook payload is invalid",
      );
    }

    request.log.info(
      {
        provider,
        webhookVerificationOutcome: "verified",
        ingestionStatus: "verified_pending_persistence",
        organizationId: payloadResult.data.organizationId,
        externalEventId: payloadResult.data.eventId,
        externalRequestId: payloadResult.data.data.externalId,
      },
      "Webhook verified successfully",
    );

    throw new AppError(
      501,
      "WEBHOOK_PERSISTENCE_NOT_IMPLEMENTED",
      "Webhook persistence is not implemented yet",
    );
  });
};
