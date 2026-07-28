import { config as loadEnvironmentFile } from "dotenv";
import { fileURLToPath } from "node:url";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { buildApp } from "../src/app.js";
import {
  WEBHOOK_PROVIDER,
  WEBHOOK_SIGNATURE_HEADER,
  WEBHOOK_TIMESTAMP_HEADER,
} from "../src/webhooks/contract.js";
import { createWebhookSignature } from "../src/webhooks/signature.js";

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

const organizationId = "10000001-0000-4000-8000-000000000001";

const unknownOrganizationId = "10000002-0000-4000-8000-000000000002";

const malformedEventPrefix = "evt-malformed-evidence-";

const app = buildApp({
  nodeEnv: "test",
  host: "127.0.0.1",
  port: 3000,
  databaseUrl,
  logLevel: "silent",
  webhookSecret,
  webhookToleranceSeconds,
});

function validPayload(): string {
  return JSON.stringify({
    organizationId,
    eventId: "evt-verification-001",
    type: "service_request.created",
    data: {
      externalId: "request-verification-001",
      requiredSkillId: "00000003-0000-4000-8000-000000000004",
      priority: "HIGH",
      region: "WEST",
    },
  });
}

function signedHeaders(
  rawBody: string | Buffer,
  timestamp = String(Math.floor(Date.now() / 1000)),
): Record<string, string> {
  return {
    "content-type": "application/json",

    [WEBHOOK_TIMESTAMP_HEADER]: timestamp,

    [WEBHOOK_SIGNATURE_HEADER]: createWebhookSignature({
      secret: webhookSecret,
      timestamp,
      rawBody: Buffer.isBuffer(rawBody)
        ? rawBody
        : Buffer.from(rawBody, "utf8"),
    }),
  };
}

beforeAll(async () => {
  await app.ready();

  await app.db.webhookEvent.deleteMany({
    where: {
      provider: WEBHOOK_PROVIDER,
      externalEventId: {
        startsWith: malformedEventPrefix,
      },
    },
  });

  await app.db.organization.upsert({
    where: {
      id: organizationId,
    },
    update: {},
    create: {
      id: organizationId,
      name: "Webhook Verification Test Organization",
    },
  });
});

afterAll(async () => {
  await app.db.webhookEvent.deleteMany({
    where: {
      organizationId,
      provider: WEBHOOK_PROVIDER,
      externalEventId: {
        startsWith: malformedEventPrefix,
      },
    },
  });

  await app.db.organization.deleteMany({
    where: {
      id: organizationId,
    },
  });

  await app.close();
});

describe("POST /webhooks/service-requests verification", () => {
  it("accepts authentication and contract validation before persistence", async () => {
    const rawBody = validPayload();

    const response = await app.inject({
      method: "POST",
      url: "/webhooks/service-requests",
      headers: signedHeaders(rawBody),
      payload: rawBody,
    });

    expect(response.statusCode).toBe(501);

    expect(response.json()).toMatchObject({
      code: "WEBHOOK_PERSISTENCE_NOT_IMPLEMENTED",
      message: "Webhook persistence is not implemented yet",
    });
  });

  it("rejects a missing signature", async () => {
    const rawBody = validPayload();
    const timestamp = String(Math.floor(Date.now() / 1000));

    const response = await app.inject({
      method: "POST",
      url: "/webhooks/service-requests",
      headers: {
        "content-type": "application/json",
        [WEBHOOK_TIMESTAMP_HEADER]: timestamp,
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
        [WEBHOOK_TIMESTAMP_HEADER]: timestamp,
        [WEBHOOK_SIGNATURE_HEADER]: "ab".repeat(31),
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
        requiredSkillId: "00000003-0000-4000-8000-000000000004",
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
        provider: WEBHOOK_PROVIDER,
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
        requiredSkillId: "00000003-0000-4000-8000-000000000004",
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
        requiredSkillId: "00000003-0000-4000-8000-000000000004",
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
        provider: WEBHOOK_PROVIDER,
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
