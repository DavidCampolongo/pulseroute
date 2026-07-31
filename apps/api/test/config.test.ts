import { describe, expect, it } from "vitest";

import { parseConfig } from "../src/config.js";

const validEnvironment = {
  NODE_ENV: "test",
  API_HOST: "127.0.0.1",
  API_PORT: "3000",
  DATABASE_URL: "postgresql://user:password@localhost:5432/pulseroute",
  LOG_LEVEL: "silent",
  WEBHOOK_SECRET: "test-webhook-secret-at-least-32-characters",
  WEBHOOK_TOLERANCE_SECONDS: "300",
};

describe("parseConfig", () => {
  it("returns typed configuration for valid environment values", () => {
    const config = parseConfig(validEnvironment);

    expect(config).toEqual({
      nodeEnv: "test",
      host: "127.0.0.1",
      port: 3000,
      databaseUrl: "postgresql://user:password@localhost:5432/pulseroute",
      logLevel: "silent",
      webhookSecret: "test-webhook-secret-at-least-32-characters",
      webhookToleranceSeconds: 300,
    });

    expect(typeof config.port).toBe("number");
    expect(typeof config.webhookToleranceSeconds).toBe("number");
  });

  it("rejects a missing required variable", () => {
    const environment = {
      ...validEnvironment,
      DATABASE_URL: undefined,
    };

    expect(() => parseConfig(environment)).toThrow(/DATABASE_URL/);
  });

  it("rejects an invalid port", () => {
    const environment = {
      ...validEnvironment,
      API_PORT: "banana",
    };

    expect(() => parseConfig(environment)).toThrow(/API_PORT/);
  });

  it("rejects an unsupported environment", () => {
    const environment = {
      ...validEnvironment,
      NODE_ENV: "dev",
    };

    expect(() => parseConfig(environment)).toThrow(/NODE_ENV/);
  });

  it("rejects an unsupported log level", () => {
    const environment = {
      ...validEnvironment,
      LOG_LEVEL: "verbose",
    };

    expect(() => parseConfig(environment)).toThrow(/LOG_LEVEL/);
  });

  it("rejects a malformed database URL", () => {
    const environment = {
      ...validEnvironment,
      DATABASE_URL: "not-a-database-url",
    };

    expect(() => parseConfig(environment)).toThrow(/DATABASE_URL/);
  });

  it("rejects a missing webhook secret", () => {
    const environment = {
      ...validEnvironment,
      WEBHOOK_SECRET: undefined,
    };

    expect(() => parseConfig(environment)).toThrow(/WEBHOOK_SECRET/);
  });

  it("rejects a webhook secret that is too short", () => {
    const environment = {
      ...validEnvironment,
      WEBHOOK_SECRET: "too-short",
    };

    expect(() => parseConfig(environment)).toThrow(/WEBHOOK_SECRET/);
  });

  it("defaults webhook freshness tolerance to 300 seconds", () => {
    const environment = {
      ...validEnvironment,
      WEBHOOK_TOLERANCE_SECONDS: undefined,
    };

    const config = parseConfig(environment);

    expect(config.webhookToleranceSeconds).toBe(300);
  });

  it("rejects an invalid webhook freshness tolerance", () => {
    const environment = {
      ...validEnvironment,
      WEBHOOK_TOLERANCE_SECONDS: "banana",
    };

    expect(() => parseConfig(environment)).toThrow(/WEBHOOK_TOLERANCE_SECONDS/);
  });

  it("rejects an excessive webhook freshness tolerance", () => {
    const environment = {
      ...validEnvironment,
      WEBHOOK_TOLERANCE_SECONDS: "3601",
    };

    expect(() => parseConfig(environment)).toThrow(/WEBHOOK_TOLERANCE_SECONDS/);
  });
});
