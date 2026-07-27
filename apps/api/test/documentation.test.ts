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

const app = buildApp({
  nodeEnv: "test",
  host: "127.0.0.1",
  port: 3000,
  databaseUrl,
  logLevel: "silent",
  webhookSecret: "test-webhook-secret-at-least-32-characters",
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

  it("serves Swagger UI", async () => {
    const response = await app.inject({
      method: "GET",
      url: "/docs/",
    });

    expect(response.statusCode).toBe(200);
    expect(response.headers["content-type"]).toContain("text/html");
  });
});
