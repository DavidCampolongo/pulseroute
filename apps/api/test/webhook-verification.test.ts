import { createHmac } from "node:crypto";
import { config as loadEnvironmentFile } from "dotenv";
import { fileURLToPath } from "node:url";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { buildApp } from "../src/app.js";

loadEnvironmentFile({
  path: fileURLToPath(new URL("../../../.env", import.meta.url)),
  quiet: true,
});

const databaseUrl = process.env.DATABASE_URL;

if (!databaseUrl) {
  throw new Error("DATABASE_URL is required for API integration tests");
}

const webhookSecret = "test-webhook-secret-at-least-32-characters";

const webhookToleranceSeconds = 300;

const webhookTimestampHeader = "x-pulseroute-timestamp";

const webhookSignatureHeader = "x-pulseroute-signature";

const webhookProvider = "pulseroute";

const organizationId = "10000001-0000-4000-8000-000000000001";

const unknownOrganizationId = "10000002-0000-4000-8000-000000000002";

const requiredSkillId = "10000003-0000-4000-8000-000000000003";

const malformedEventPrefix = "evt-malformed-evidence-";

const rollbackExternalRequestId = "request-rollback-injection-001";

const app = buildApp({
  nodeEnv: "test",
  host: "127.0.0.1",
  port: 3000,
  databaseUrl,
  logLevel: "silent",
  webhookSecret,
  webhookToleranceSeconds,
});

function validPayload(
  options: {
    eventId?: string;
    externalId?: string;
  } = {},
): string {
  return JSON.stringify({
    organizationId,
    eventId: options.eventId ?? "evt-verification-001",
    type: "service_request.created",
    data: {
      externalId: options.externalId ?? "request-verification-001",
      requiredSkillId,
      priority: "HIGH",
      region: "WEST",
    },
  });
}

function createIndependentWebhookSignature(
  rawBody: string | Buffer,
  timestamp: string,
): string {
  const bodyBytes = Buffer.isBuffer(rawBody)
    ? rawBody
    : Buffer.from(rawBody, "utf8");

  return createHmac("sha256", webhookSecret)
    .update(timestamp, "utf8")
    .update(".", "utf8")
    .update(bodyBytes)
    .digest("hex");
}

function signedHeaders(
  rawBody: string | Buffer,
  timestamp = String(Math.floor(Date.now() / 1000)),
): Record<string, string> {
  return {
    "content-type": "application/json",

    [webhookTimestampHeader]: timestamp,

    [webhookSignatureHeader]: createIndependentWebhookSignature(
      rawBody,
      timestamp,
    ),
  };
}

async function removeRollbackConstraint(): Promise<void> {
  await app.db.$executeRaw`
    ALTER TABLE "outbox_events"
    DROP CONSTRAINT IF EXISTS "outbox_test_rollback_failure"
  `;
}

async function installRollbackConstraint(): Promise<void> {
  await removeRollbackConstraint();

  await app.db.$executeRaw`
    ALTER TABLE "outbox_events"
    ADD CONSTRAINT "outbox_test_rollback_failure"
    CHECK (
      (payload ->> 'externalId')
      IS DISTINCT FROM 'request-rollback-injection-001'
    )
  `;
}

async function clearTestOrganizationData(): Promise<void> {
  await app.db.auditLog.deleteMany({
    where: {
      organizationId,
    },
  });

  await app.db.outboxEvent.deleteMany({
    where: {
      organizationId,
    },
  });

  await app.db.webhookEvent.deleteMany({
    where: {
      organizationId,
    },
  });

  await app.db.serviceRequest.deleteMany({
    where: {
      organizationId,
    },
  });

  await app.db.skill.deleteMany({
    where: {
      organizationId,
    },
  });

  await app.db.organization.deleteMany({
    where: {
      id: organizationId,
    },
  });
}

beforeAll(async () => {
  await app.ready();

  await removeRollbackConstraint();

  await clearTestOrganizationData();

  await app.db.organization.create({
    data: {
      id: organizationId,
      name: "Webhook Verification Test Organization",
    },
  });

  await app.db.skill.create({
    data: {
      id: requiredSkillId,
      organizationId,
      name: "Webhook Verification Test Skill",
    },
  });
});

afterAll(async () => {
  await removeRollbackConstraint();

  await clearTestOrganizationData();

  await app.close();
});

describe("POST /webhooks/service-requests verification", () => {
  it("persists a valid authenticated webhook atomically", async () => {
    const rawBody = validPayload();

    const response = await app.inject({
      method: "POST",
      url: "/webhooks/service-requests",
      headers: signedHeaders(rawBody),
      payload: rawBody,
    });

    expect(response.statusCode).toBe(202);

    const responseBody = response.json();

    expect(responseBody).toMatchObject({
      status: "accepted",
    });

    expect(responseBody.requestId).toEqual(expect.any(String));

    expect(responseBody.serviceRequestId).toEqual(expect.any(String));

    const serviceRequestId = responseBody.serviceRequestId as string;

    const serviceRequests = await app.db.serviceRequest.findMany({
      where: {
        organizationId,
        externalId: "request-verification-001",
      },
    });

    expect(serviceRequests).toHaveLength(1);

    const [serviceRequest] = serviceRequests;

    if (!serviceRequest) {
      throw new Error("Expected accepted ServiceRequest to exist");
    }

    expect(serviceRequest.id).toBe(serviceRequestId);
    expect(serviceRequest.requiredSkillId).toBe(requiredSkillId);
    expect(serviceRequest.status).toBe("PENDING");
    expect(serviceRequest.priority).toBe("HIGH");
    expect(serviceRequest.region).toBe("WEST");

    const webhookEvents = await app.db.webhookEvent.findMany({
      where: {
        organizationId,
        provider: webhookProvider,
        externalEventId: "evt-verification-001",
      },
    });

    expect(webhookEvents).toHaveLength(1);

    const [webhookEvent] = webhookEvents;

    if (!webhookEvent) {
      throw new Error("Expected accepted WebhookEvent to exist");
    }

    expect(webhookEvent.status).toBe("PROCESSED");
    expect(webhookEvent.serviceRequestId).toBe(serviceRequestId);
    expect(webhookEvent.malformedReason).toBeNull();

    expect(
      Buffer.from(webhookEvent.rawBody).equals(Buffer.from(rawBody, "utf8")),
    ).toBe(true);

    expect(webhookEvent.parsedPayload).toEqual(JSON.parse(rawBody));

    const outboxEvents = await app.db.outboxEvent.findMany({
      where: {
        organizationId,
        aggregateType: "service_request",
        aggregateId: serviceRequestId,
      },
    });

    expect(outboxEvents).toHaveLength(1);

    const [outboxEvent] = outboxEvents;

    if (!outboxEvent) {
      throw new Error("Expected accepted OutboxEvent to exist");
    }

    expect(outboxEvent.eventType).toBe("service_request.created");
    expect(outboxEvent.status).toBe("PENDING");

    expect(outboxEvent.payload).toEqual({
      serviceRequestId,
      externalId: "request-verification-001",
      requiredSkillId,
      priority: "HIGH",
      region: "WEST",
    });

    const auditLogs = await app.db.auditLog.findMany({
      where: {
        organizationId,
        action: "service_request.ingested",
        entityType: "service_request",
        entityId: serviceRequestId,
      },
    });

    expect(auditLogs).toHaveLength(1);

    const [auditLog] = auditLogs;

    if (!auditLog) {
      throw new Error("Expected accepted AuditLog to exist");
    }

    expect(auditLog.actorType).toBe("SYSTEM");
    expect(auditLog.actorUserId).toBeNull();
    expect(auditLog.correlationId).toBe(responseBody.requestId);

    expect(auditLog.metadata).toEqual({
      provider: webhookProvider,
      externalEventId: "evt-verification-001",
      webhookEventId: webhookEvent.id,
      outboxEventId: outboxEvent.id,
    });
  });

  it("treats a sequential replay as successful without creating duplicate state", async () => {
    const externalEventId = "evt-duplicate-replay-001";
    const externalRequestId = "request-duplicate-replay-001";

    const rawBody = validPayload({
      eventId: externalEventId,
      externalId: externalRequestId,
    });

    const firstResponse = await app.inject({
      method: "POST",
      url: "/webhooks/service-requests",
      headers: signedHeaders(rawBody),
      payload: rawBody,
    });

    expect(firstResponse.statusCode).toBe(202);

    const firstBody = firstResponse.json();

    expect(firstBody).toMatchObject({
      status: "accepted",
    });

    expect(firstBody.serviceRequestId).toEqual(expect.any(String));

    const secondResponse = await app.inject({
      method: "POST",
      url: "/webhooks/service-requests",
      headers: signedHeaders(rawBody),
      payload: rawBody,
    });

    expect(secondResponse.statusCode).toBe(200);

    const secondBody = secondResponse.json();

    expect(secondBody).toMatchObject({
      status: "duplicate",
      serviceRequestId: firstBody.serviceRequestId,
    });

    expect(secondBody.requestId).toEqual(expect.any(String));

    const serviceRequests = await app.db.serviceRequest.findMany({
      where: {
        organizationId,
        externalId: externalRequestId,
      },
    });

    expect(serviceRequests).toHaveLength(1);

    const [serviceRequest] = serviceRequests;

    if (!serviceRequest) {
      throw new Error("Expected duplicate replay ServiceRequest to exist");
    }

    expect(serviceRequest.id).toBe(firstBody.serviceRequestId);

    const webhookEvents = await app.db.webhookEvent.findMany({
      where: {
        organizationId,
        provider: webhookProvider,
        externalEventId,
      },
    });

    expect(webhookEvents).toHaveLength(1);

    const outboxEvents = await app.db.outboxEvent.findMany({
      where: {
        organizationId,
        aggregateType: "service_request",
        aggregateId: serviceRequest.id,
      },
    });

    expect(outboxEvents).toHaveLength(1);

    const auditLogs = await app.db.auditLog.findMany({
      where: {
        organizationId,
        action: "service_request.ingested",
        entityType: "service_request",
        entityId: serviceRequest.id,
      },
    });

    expect(auditLogs).toHaveLength(1);
  });

  it("handles 20 concurrent duplicate deliveries with exactly one accepted state", async () => {
    const externalEventId = "evt-concurrent-duplicate-001";
    const externalRequestId = "request-concurrent-duplicate-001";

    const rawBody = validPayload({
      eventId: externalEventId,
      externalId: externalRequestId,
    });

    const timestamp = String(Math.floor(Date.now() / 1000));

    const headers = signedHeaders(rawBody, timestamp);

    const responses = await Promise.all(
      Array.from({ length: 20 }, () =>
        app.inject({
          method: "POST",
          url: "/webhooks/service-requests",
          headers: {
            ...headers,
          },
          payload: rawBody,
        }),
      ),
    );

    const acceptedResponses = responses.filter(
      (response) => response.statusCode === 202,
    );

    const duplicateResponses = responses.filter(
      (response) => response.statusCode === 200,
    );

    expect(acceptedResponses).toHaveLength(1);
    expect(duplicateResponses).toHaveLength(19);

    const responseBodies = responses.map((response) => response.json());

    for (const responseBody of responseBodies) {
      expect(responseBody.requestId).toEqual(expect.any(String));

      expect(responseBody.serviceRequestId).toEqual(expect.any(String));
    }

    const acceptedBodies = responseBodies.filter(
      (responseBody) => responseBody.status === "accepted",
    );

    const duplicateBodies = responseBodies.filter(
      (responseBody) => responseBody.status === "duplicate",
    );

    expect(acceptedBodies).toHaveLength(1);
    expect(duplicateBodies).toHaveLength(19);

    const serviceRequestIds = new Set(
      responseBodies.map(
        (responseBody) => responseBody.serviceRequestId as string,
      ),
    );

    expect(serviceRequestIds.size).toBe(1);

    const serviceRequests = await app.db.serviceRequest.findMany({
      where: {
        organizationId,
        externalId: externalRequestId,
      },
    });

    expect(serviceRequests).toHaveLength(1);

    const [serviceRequest] = serviceRequests;

    if (!serviceRequest) {
      throw new Error("Expected concurrent duplicate ServiceRequest to exist");
    }

    expect(serviceRequestIds.has(serviceRequest.id)).toBe(true);
    expect(serviceRequest.status).toBe("PENDING");

    const webhookEvents = await app.db.webhookEvent.findMany({
      where: {
        organizationId,
        provider: webhookProvider,
        externalEventId,
      },
    });

    expect(webhookEvents).toHaveLength(1);

    const [webhookEvent] = webhookEvents;

    if (!webhookEvent) {
      throw new Error("Expected concurrent duplicate WebhookEvent to exist");
    }

    expect(webhookEvent.status).toBe("PROCESSED");
    expect(webhookEvent.serviceRequestId).toBe(serviceRequest.id);

    const outboxEvents = await app.db.outboxEvent.findMany({
      where: {
        organizationId,
        aggregateType: "service_request",
        aggregateId: serviceRequest.id,
      },
    });

    expect(outboxEvents).toHaveLength(1);

    const [outboxEvent] = outboxEvents;

    if (!outboxEvent) {
      throw new Error("Expected concurrent duplicate OutboxEvent to exist");
    }

    expect(outboxEvent.status).toBe("PENDING");

    const auditLogs = await app.db.auditLog.findMany({
      where: {
        organizationId,
        action: "service_request.ingested",
        entityType: "service_request",
        entityId: serviceRequest.id,
      },
    });

    expect(auditLogs).toHaveLength(1);
  });

  it("rolls back all accepted state when the outbox insert fails", async () => {
    const externalEventId = "evt-rollback-injection-001";

    const rawBody = validPayload({
      eventId: externalEventId,
      externalId: rollbackExternalRequestId,
    });

    const before = {
      serviceRequests: await app.db.serviceRequest.count({
        where: {
          organizationId,
        },
      }),

      webhookEvents: await app.db.webhookEvent.count({
        where: {
          organizationId,
        },
      }),

      outboxEvents: await app.db.outboxEvent.count({
        where: {
          organizationId,
        },
      }),

      auditLogs: await app.db.auditLog.count({
        where: {
          organizationId,
        },
      }),
    };

    await installRollbackConstraint();

    const response = await (async () => {
      try {
        return await app.inject({
          method: "POST",
          url: "/webhooks/service-requests",
          headers: signedHeaders(rawBody),
          payload: rawBody,
        });
      } finally {
        await removeRollbackConstraint();
      }
    })();

    expect(response.statusCode).toBe(500);

    expect(response.json()).toMatchObject({
      code: "INTERNAL_ERROR",
    });

    const serviceRequest = await app.db.serviceRequest.findUnique({
      where: {
        organizationId_externalId: {
          organizationId,
          externalId: rollbackExternalRequestId,
        },
      },
    });

    expect(serviceRequest).toBeNull();

    const webhookEvent = await app.db.webhookEvent.findFirst({
      where: {
        organizationId,
        provider: webhookProvider,
        externalEventId,
      },
    });

    expect(webhookEvent).toBeNull();

    const after = {
      serviceRequests: await app.db.serviceRequest.count({
        where: {
          organizationId,
        },
      }),

      webhookEvents: await app.db.webhookEvent.count({
        where: {
          organizationId,
        },
      }),

      outboxEvents: await app.db.outboxEvent.count({
        where: {
          organizationId,
        },
      }),

      auditLogs: await app.db.auditLog.count({
        where: {
          organizationId,
        },
      }),
    };

    expect(after).toEqual(before);
  });

  it("rejects a missing signature", async () => {
    const rawBody = validPayload();
    const timestamp = String(Math.floor(Date.now() / 1000));

    const response = await app.inject({
      method: "POST",
      url: "/webhooks/service-requests",
      headers: {
        "content-type": "application/json",
        [webhookTimestampHeader]: timestamp,
      },
      payload: rawBody,
    });

    expect(response.statusCode).toBe(401);

    expect(response.json()).toMatchObject({
      code: "WEBHOOK_AUTHENTICATION_FAILED",
    });
  });

  it("rejects a malformed signature length safely", async () => {
    const rawBody = validPayload();
    const timestamp = String(Math.floor(Date.now() / 1000));

    const response = await app.inject({
      method: "POST",
      url: "/webhooks/service-requests",
      headers: {
        "content-type": "application/json",
        [webhookTimestampHeader]: timestamp,
        [webhookSignatureHeader]: "ab".repeat(31),
      },
      payload: rawBody,
    });

    expect(response.statusCode).toBe(401);

    expect(response.json()).toMatchObject({
      code: "WEBHOOK_AUTHENTICATION_FAILED",
    });
  });

  it("rejects a stale signed request", async () => {
    const rawBody = validPayload();

    const timestamp = String(
      Math.floor(Date.now() / 1000) - webhookToleranceSeconds - 10,
    );

    const response = await app.inject({
      method: "POST",
      url: "/webhooks/service-requests",
      headers: signedHeaders(rawBody, timestamp),
      payload: rawBody,
    });

    expect(response.statusCode).toBe(401);

    expect(response.json()).toMatchObject({
      code: "WEBHOOK_AUTHENTICATION_FAILED",
    });
  });

  it("rejects a body changed after signing", async () => {
    const originalBody = validPayload();

    const timestamp = String(Math.floor(Date.now() / 1000));

    const headers = signedHeaders(originalBody, timestamp);

    const tamperedBody = originalBody.replace('"WEST"', '"EAST"');

    expect(tamperedBody).not.toBe(originalBody);

    const response = await app.inject({
      method: "POST",
      url: "/webhooks/service-requests",
      headers,
      payload: tamperedBody,
    });

    expect(response.statusCode).toBe(401);

    expect(response.json()).toMatchObject({
      code: "WEBHOOK_AUTHENTICATION_FAILED",
    });
  });

  it("rejects authentic invalid JSON after authentication", async () => {
    const rawBody = '{"organizationId":"00000001-0000-4000-8000-000000000001"';

    const response = await app.inject({
      method: "POST",
      url: "/webhooks/service-requests",
      headers: signedHeaders(rawBody),
      payload: rawBody,
    });

    expect(response.statusCode).toBe(400);

    expect(response.json()).toMatchObject({
      code: "WEBHOOK_PAYLOAD_INVALID",
      message: "Webhook payload is invalid",
    });
  });

  it("rejects authentic invalid UTF-8 after authentication", async () => {
    const rawBody = Buffer.concat([
      Buffer.from('{"value":"', "utf8"),
      Buffer.from([0xc3, 0x28]),
      Buffer.from('"}', "utf8"),
    ]);

    const response = await app.inject({
      method: "POST",
      url: "/webhooks/service-requests",
      headers: signedHeaders(rawBody),
      payload: rawBody,
    });

    expect(response.statusCode).toBe(400);

    expect(response.json()).toMatchObject({
      code: "WEBHOOK_PAYLOAD_INVALID",
      message: "Webhook payload is invalid",
    });
  });

  it("retains authentic malformed service-request evidence without creating business state", async () => {
    const externalEventId = `${malformedEventPrefix}contract-001`;

    const externalRequestId = "request-malformed-evidence-001";

    const rawBody = JSON.stringify({
      organizationId,
      eventId: externalEventId,
      type: "service_request.created",
      data: {
        externalId: externalRequestId,
        requiredSkillId,
        priority: "URGENT",
        region: "WEST",
      },
    });

    const response = await app.inject({
      method: "POST",
      url: "/webhooks/service-requests",
      headers: signedHeaders(rawBody),
      payload: rawBody,
    });

    expect(response.statusCode).toBe(400);

    expect(response.json()).toMatchObject({
      code: "WEBHOOK_PAYLOAD_INVALID",
      message: "Webhook payload is invalid",
    });

    const webhookEvents = await app.db.webhookEvent.findMany({
      where: {
        organizationId,
        provider: webhookProvider,
        externalEventId,
      },
    });

    expect(webhookEvents).toHaveLength(1);

    const [webhookEvent] = webhookEvents;

    if (!webhookEvent) {
      throw new Error("Expected malformed webhook evidence to be persisted");
    }

    expect(webhookEvent.status).toBe("MALFORMED");
    expect(webhookEvent.serviceRequestId).toBeNull();
    expect(webhookEvent.malformedReason).toBe(
      "SERVICE_REQUEST_CONTRACT_INVALID",
    );

    expect(
      Buffer.from(webhookEvent.rawBody).equals(Buffer.from(rawBody, "utf8")),
    ).toBe(true);

    expect(webhookEvent.parsedPayload).toEqual({
      organizationId,
      eventId: externalEventId,
      type: "service_request.created",
      data: {
        externalId: externalRequestId,
        requiredSkillId,
        priority: "URGENT",
        region: "WEST",
      },
    });

    const serviceRequest = await app.db.serviceRequest.findFirst({
      where: {
        organizationId,
        externalId: externalRequestId,
      },
    });

    expect(serviceRequest).toBeNull();
  });

  it("does not falsely attribute malformed evidence to an unknown organization", async () => {
    const externalEventId = `${malformedEventPrefix}unknown-organization-001`;

    const rawBody = JSON.stringify({
      organizationId: unknownOrganizationId,
      eventId: externalEventId,
      type: "service_request.created",
      data: {
        externalId: "request-unknown-organization-001",
        requiredSkillId,
        priority: "URGENT",
        region: "WEST",
      },
    });

    const response = await app.inject({
      method: "POST",
      url: "/webhooks/service-requests",
      headers: signedHeaders(rawBody),
      payload: rawBody,
    });

    expect(response.statusCode).toBe(400);

    expect(response.json()).toMatchObject({
      code: "WEBHOOK_PAYLOAD_INVALID",
      message: "Webhook payload is invalid",
    });

    const webhookEvent = await app.db.webhookEvent.findFirst({
      where: {
        provider: webhookProvider,
        externalEventId,
      },
    });

    expect(webhookEvent).toBeNull();
  });

  it("rejects non-JSON content types safely", async () => {
    const response = await app.inject({
      method: "POST",
      url: "/webhooks/service-requests",
      headers: {
        "content-type": "text/plain",
      },
      payload: "not-json",
    });

    expect(response.statusCode).toBe(415);

    expect(response.json()).toMatchObject({
      code: "WEBHOOK_CONTENT_TYPE_UNSUPPORTED",
      message: "Webhook requests must use application/json",
    });
  });
});
