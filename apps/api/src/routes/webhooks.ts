import { isUtf8 } from "node:buffer";

import type { Prisma } from "@pulseroute/db";
import type { FastifyPluginAsync, FastifyRequest } from "fastify";

import { AppError } from "../errors.js";
import { registerWebhookRawBodyParser } from "../plugins/webhook-raw-body.js";
import {
  SERVICE_REQUEST_EVENT_TYPE,
  WEBHOOK_PROVIDER,
  WEBHOOK_SIGNATURE_HEADER,
  WEBHOOK_TIMESTAMP_HEADER,
  serviceRequestWebhookSchema,
  toServiceRequestCreateValues,
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

  app.post("/service-requests", async (request, reply) => {
    if (!Buffer.isBuffer(request.body) || !Buffer.isBuffer(request.rawBody)) {
      throw new AppError(
        415,
        "WEBHOOK_CONTENT_TYPE_UNSUPPORTED",
        "Webhook requests must use application/json",
      );
    }

    const rawBody = request.rawBody;

    const timestamp = readHeader(request, WEBHOOK_TIMESTAMP_HEADER);

    const signature = readHeader(request, WEBHOOK_SIGNATURE_HEADER);

    const verification = verifyWebhookSignature({
      secret: options.webhookSecret,
      timestamp,
      signature,
      rawBody: rawBody,
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
      parsedPayload = parseAuthenticatedJson(rawBody);
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
          rawBody: Uint8Array.from(rawBody),
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

    const serviceRequestValues = toServiceRequestCreateValues(
      payloadResult.data,
    );

    const accepted = await app.db.$transaction(async (transaction) => {
      const serviceRequest = await transaction.serviceRequest.create({
        data: serviceRequestValues,
        select: {
          id: true,
        },
      });

      const webhookEvent = await transaction.webhookEvent.create({
        data: {
          organizationId: payloadResult.data.organizationId,
          serviceRequestId: serviceRequest.id,
          provider: WEBHOOK_PROVIDER,
          externalEventId: payloadResult.data.eventId,
          status: "PROCESSED",
          rawBody: Uint8Array.from(rawBody),
          parsedPayload: payloadResult.data as Prisma.InputJsonValue,
        },
        select: {
          id: true,
        },
      });

      const outboxEvent = await transaction.outboxEvent.create({
        data: {
          organizationId: payloadResult.data.organizationId,
          eventType: SERVICE_REQUEST_EVENT_TYPE,
          aggregateType: "service_request",
          aggregateId: serviceRequest.id,
          payload: {
            serviceRequestId: serviceRequest.id,
            externalId: payloadResult.data.data.externalId,
            requiredSkillId: payloadResult.data.data.requiredSkillId,
            priority: payloadResult.data.data.priority,
            region: payloadResult.data.data.region,
          },
        },
        select: {
          id: true,
        },
      });

      const auditLog = await transaction.auditLog.create({
        data: {
          organizationId: payloadResult.data.organizationId,
          actorType: "SYSTEM",
          action: "service_request.ingested",
          entityType: "service_request",
          entityId: serviceRequest.id,
          correlationId: request.id,
          metadata: {
            provider: WEBHOOK_PROVIDER,
            externalEventId: payloadResult.data.eventId,
            webhookEventId: webhookEvent.id,
            outboxEventId: outboxEvent.id,
          },
        },
        select: {
          id: true,
        },
      });

      return {
        serviceRequestId: serviceRequest.id,
        webhookEventId: webhookEvent.id,
        outboxEventId: outboxEvent.id,
        auditLogId: auditLog.id,
      };
    });

    request.log.info(
      {
        provider: WEBHOOK_PROVIDER,
        webhookVerificationOutcome: "verified",
        ingestionStatus: "accepted",
        organizationId: payloadResult.data.organizationId,
        externalEventId: payloadResult.data.eventId,
        externalRequestId: payloadResult.data.data.externalId,
        serviceRequestId: accepted.serviceRequestId,
        webhookEventId: accepted.webhookEventId,
        outboxEventId: accepted.outboxEventId,
        auditLogId: accepted.auditLogId,
      },
      "Webhook ingested successfully",
    );

    return reply.code(202).send({
      requestId: request.id,
      status: "accepted",
      serviceRequestId: accepted.serviceRequestId,
    });
  });
};
