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
});

let validationHandlerRan = false;
let capturedRequestId: string | undefined;

app.addHook("onRequest", async (request) => {
  capturedRequestId = request.id;
});

app.post(
  "/test/validation",
  {
    schema: {
      body: {
        type: "object",
        additionalProperties: false,
        required: ["name"],
        properties: {
          name: {
            type: "string",
          },
        },
      },
    },
  },
  async () => {
    validationHandlerRan = true;

    return {
      ok: true,
    };
  },
);

app.get("/test/unexpected-error", async () => {
  throw new Error("Secret internal failure");
});

beforeAll(async () => {
  await app.ready();
});

afterAll(async () => {
  await app.close();
});

describe("central error handling", () => {
  it("rejects invalid input before the handler runs", async () => {
    validationHandlerRan = false;

    const response = await app.inject({
      method: "POST",
      url: "/test/validation",
      payload: {},
    });

    expect(response.statusCode).toBe(400);
    expect(validationHandlerRan).toBe(false);

    expect(response.json()).toMatchObject({
      requestId: capturedRequestId,
      code: "VALIDATION_ERROR",
      message: "Request validation failed",
    });
  });

  it("returns a safe response for unexpected errors", async () => {
    const response = await app.inject({
      method: "GET",
      url: "/test/unexpected-error",
    });

    expect(response.statusCode).toBe(500);

    expect(response.json()).toEqual({
      requestId: capturedRequestId,
      code: "INTERNAL_ERROR",
      message: "An unexpected error occurred",
    });

    expect(response.body).not.toContain("Secret internal failure");
    expect(response.body).not.toContain("stack");
  });
});
