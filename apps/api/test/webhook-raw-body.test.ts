import { config as loadEnvironmentFile } from "dotenv";
import Fastify from "fastify";
import { fileURLToPath } from "node:url";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { buildApp } from "../src/app.js";
import { registerWebhookRawBodyParser } from "../src/plugins/webhook-raw-body.js";

loadEnvironmentFile({
  path: fileURLToPath(new URL("../../../.env", import.meta.url)),
  quiet: true,
});

const databaseUrl = process.env.DATABASE_URL;

if (!databaseUrl) {
  throw new Error("DATABASE_URL is required for API integration tests");
}

describe("webhook raw-body parser", () => {
  it("preserves the exact JSON bytes as a Buffer", async () => {
    const app = Fastify({
      logger: false,
    });

    app.register(async (webhookScope) => {
      registerWebhookRawBodyParser(webhookScope);

      webhookScope.post("/probe", async (request) => {
        return {
          bodyIsBuffer: Buffer.isBuffer(request.body),
          rawBodyIsBuffer: Buffer.isBuffer(request.rawBody),
          rawBodyText: request.rawBody?.toString("utf8"),
        };
      });
    });

    await app.ready();

    try {
      const payload = '{ "a": 1, "b": 2 }';

      const response = await app.inject({
        method: "POST",
        url: "/probe",
        headers: {
          "content-type": "application/json",
        },
        payload,
      });

      expect(response.statusCode).toBe(200);

      expect(response.json()).toEqual({
        bodyIsBuffer: true,
        rawBodyIsBuffer: true,
        rawBodyText: payload,
      });
    } finally {
      await app.close();
    }
  });

  it("preserves representation differences between equivalent JSON", async () => {
    const app = Fastify({
      logger: false,
    });

    app.register(async (webhookScope) => {
      registerWebhookRawBodyParser(webhookScope);

      webhookScope.post("/probe", async (request) => {
        if (!request.rawBody) {
          throw new Error("Expected raw webhook body");
        }

        return {
          rawBodyHex: request.rawBody.toString("hex"),
        };
      });
    });

    await app.ready();

    try {
      const compactPayload = '{"a":1,"b":2}';
      const spacedPayload = '{ "a": 1, "b": 2 }';

      const compactResponse = await app.inject({
        method: "POST",
        url: "/probe",
        headers: {
          "content-type": "application/json",
        },
        payload: compactPayload,
      });

      const spacedResponse = await app.inject({
        method: "POST",
        url: "/probe",
        headers: {
          "content-type": "application/json",
        },
        payload: spacedPayload,
      });

      expect(JSON.parse(compactPayload)).toEqual(JSON.parse(spacedPayload));

      expect(compactResponse.json().rawBodyHex).not.toBe(
        spacedResponse.json().rawBodyHex,
      );
    } finally {
      await app.close();
    }
  });

  it("does not change normal JSON parsing outside the webhook scope", async () => {
    const app = Fastify({
      logger: false,
    });

    app.post("/normal", async (request) => {
      return {
        bodyIsBuffer: Buffer.isBuffer(request.body),
        body: request.body,
        hasRawBody: request.rawBody != null,
      };
    });

    app.register(async (webhookScope) => {
      registerWebhookRawBodyParser(webhookScope);

      webhookScope.post("/webhook", async (request) => {
        return {
          bodyIsBuffer: Buffer.isBuffer(request.body),
        };
      });
    });

    await app.ready();

    try {
      const normalResponse = await app.inject({
        method: "POST",
        url: "/normal",
        payload: {
          name: "PulseRoute",
        },
      });

      expect(normalResponse.statusCode).toBe(200);

      expect(normalResponse.json()).toEqual({
        bodyIsBuffer: false,
        body: {
          name: "PulseRoute",
        },
        hasRawBody: false,
      });

      const webhookResponse = await app.inject({
        method: "POST",
        url: "/webhook",
        headers: {
          "content-type": "application/json",
        },
        payload: '{"name":"PulseRoute"}',
      });

      expect(webhookResponse.statusCode).toBe(200);

      expect(webhookResponse.json()).toEqual({
        bodyIsBuffer: true,
      });
    } finally {
      await app.close();
    }
  });
});

const integratedApp = buildApp({
  nodeEnv: "test",
  host: "127.0.0.1",
  port: 3000,
  databaseUrl,
  logLevel: "silent",
  webhookSecret: "test-webhook-secret-at-least-32-characters",
  webhookToleranceSeconds: 300,
});

beforeAll(async () => {
  await integratedApp.ready();
});

afterAll(async () => {
  await integratedApp.close();
});

describe("POST /webhooks/service-requests raw-body integration", () => {
  it("rejects an unsigned webhook after raw-body capture", async () => {
    const response = await integratedApp.inject({
      method: "POST",
      url: "/webhooks/service-requests",
      headers: {
        "content-type": "application/json",
      },
      payload: '{ "externalId": "raw-body-proof" }',
    });

    expect(response.statusCode).toBe(401);

    expect(response.json()).toMatchObject({
      code: "WEBHOOK_AUTHENTICATION_FAILED",
      message: "Webhook authentication failed",
    });
  });
});
