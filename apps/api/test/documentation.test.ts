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

const app = buildApp({
  nodeEnv: "test",
  host: "127.0.0.1",
  port: 3000,
  databaseUrl,
  logLevel: "silent",
  webhookSecret,
  webhookToleranceSeconds: 300,
});

beforeAll(async () => {
  await app.ready();
});

afterAll(async () => {
  await app.close();
});

describe("API documentation", () => {
  it("exposes OpenAPI JSON containing the health route", async () => {
    const response = await app.inject({
      method: "GET",
      url: "/docs/json",
    });

    expect(response.statusCode).toBe(200);

    const document = response.json();

    expect(document.openapi).toBe("3.0.3");
    expect(document.paths).toHaveProperty("/health");
    expect(document.paths["/health"]).toHaveProperty("get");
  });

  it("documents the signed service-request webhook contract", async () => {
    const response = await app.inject({
      method: "GET",
      url: "/docs/json",
    });

    expect(response.statusCode).toBe(200);

    const document = response.json();

    expect(document.paths).toHaveProperty("/webhooks/service-requests");

    const operation = document.paths["/webhooks/service-requests"].post;

    expect(operation).toMatchObject({
      operationId: "ingestServiceRequestWebhook",
      tags: ["Webhooks"],
    });

    const requestSchema =
      operation.requestBody.content["application/json"].schema;

    expect(requestSchema).toMatchObject({
      type: "object",
      additionalProperties: false,
    });

    expect(requestSchema.required).toEqual(
      expect.arrayContaining(["organizationId", "eventId", "type", "data"]),
    );

    expect(requestSchema.properties).toHaveProperty("organizationId");

    expect(requestSchema.properties).toHaveProperty("eventId");

    expect(requestSchema.properties).toHaveProperty("type");

    expect(requestSchema.properties).toHaveProperty("data");

    const headerParameters = (
      operation.parameters as Array<{
        in?: string;
        name?: string;
        required?: boolean;
      }>
    ).filter((parameter) => parameter.in === "header");

    expect(headerParameters).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          in: "header",
          name: "x-pulseroute-timestamp",
          required: true,
        }),

        expect.objectContaining({
          in: "header",
          name: "x-pulseroute-signature",
          required: true,
        }),
      ]),
    );

    expect(operation.responses).toHaveProperty("200");
    expect(operation.responses).toHaveProperty("202");
    expect(operation.responses).toHaveProperty("400");
    expect(operation.responses).toHaveProperty("401");
    expect(operation.responses).toHaveProperty("415");
    expect(operation.responses).toHaveProperty("500");

    expect(JSON.stringify(document)).not.toContain(webhookSecret);
  });

  it("serves Swagger UI", async () => {
    const response = await app.inject({
      method: "GET",
      url: "/docs/",
    });

    expect(response.statusCode).toBe(200);
    expect(response.headers["content-type"]).toContain("text/html");
  });
});
