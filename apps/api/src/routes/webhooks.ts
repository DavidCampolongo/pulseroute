import { isUtf8 } from "node:buffer";

import type { Prisma } from "@pulseroute/db";
import type { FastifyPluginAsync, FastifyRequest } from "fastify";

import { AppError } from "../errors.js";
import { registerWebhookRawBodyParser } from "../plugins/webhook-raw-body.js";
import {
  WEBHOOK_PROVIDER,
  WEBHOOK_SIGNATURE_HEADER,
  WEBHOOK_TIMESTAMP_HEADER,
  serviceRequestWebhookSchema,
  webhookEvidenceIdentitySchema,
} from "../webhooks/contract.js";
import { verifyWebhookSignature } from "../webhooks/signature.js";

type WebhookRouteOptions = {
  webhookSecret: string;
  webhookToleranceSeconds: number;
};

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
          provider: WEBHOOK_PROVIDER,
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
          provider: WEBHOOK_PROVIDER,
          webhookVerificationOutcome: "verified",
          ingestionStatus: "invalid_json",
        },
        "Authenticated webhook contained invalid JSON",
      );

      throw error;
    }

    const payloadResult = serviceRequestWebhookSchema.safeParse(parsedPayload);

    if (!payloadResult.success) {
      const evidenceIdentity =
        webhookEvidenceIdentitySchema.safeParse(parsedPayload);

      if (!evidenceIdentity.success) {
        request.log.warn(
          {
            provider: WEBHOOK_PROVIDER,
            webhookVerificationOutcome: "verified",
            ingestionStatus: "invalid_payload_unattributed",
            payloadValidationIssueCount: payloadResult.error.issues.length,
          },
          "Authenticated malformed webhook could not be attributed",
        );

        throw new AppError(
          400,
          "WEBHOOK_PAYLOAD_INVALID",
          "Webhook payload is invalid",
        );
      }

      const organization = await app.db.organization.findUnique({
        where: {
          id: evidenceIdentity.data.organizationId,
        },
        select: {
          id: true,
        },
      });

      if (!organization) {
        request.log.warn(
          {
            provider: WEBHOOK_PROVIDER,
            webhookVerificationOutcome: "verified",
            ingestionStatus: "invalid_payload_unknown_organization",
            organizationId: evidenceIdentity.data.organizationId,
            externalEventId: evidenceIdentity.data.eventId,
            payloadValidationIssueCount: payloadResult.error.issues.length,
          },
          "Authenticated malformed webhook referenced an unknown organization",
        );

        throw new AppError(
          400,
          "WEBHOOK_PAYLOAD_INVALID",
          "Webhook payload is invalid",
        );
      }

      const malformedEvent = await app.db.webhookEvent.create({
        data: {
          organizationId: organization.id,
          provider: WEBHOOK_PROVIDER,
          externalEventId: evidenceIdentity.data.eventId ?? null,
          status: "MALFORMED",
          rawBody: Uint8Array.from(request.rawBody),
          parsedPayload: parsedPayload as Prisma.InputJsonValue,
          malformedReason: "SERVICE_REQUEST_CONTRACT_INVALID",
        },
        select: {
          id: true,
        },
      });

      request.log.warn(
        {
          provider: WEBHOOK_PROVIDER,
          webhookVerificationOutcome: "verified",
          ingestionStatus: "malformed_retained",
          organizationId: organization.id,
          externalEventId: evidenceIdentity.data.eventId,
          webhookEventId: malformedEvent.id,
          payloadValidationIssueCount: payloadResult.error.issues.length,
        },
        "Authenticated malformed webhook evidence retained",
      );

      throw new AppError(
        400,
        "WEBHOOK_PAYLOAD_INVALID",
        "Webhook payload is invalid",
      );
    }

    request.log.info(
      {
        provider: WEBHOOK_PROVIDER,
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
