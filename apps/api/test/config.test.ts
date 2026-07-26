import { describe, expect, it } from "vitest";

import { parseConfig } from "../src/config.js";

const validEnvironment = {
  NODE_ENV: "test",
  API_HOST: "127.0.0.1",
  API_PORT: "3000",
  DATABASE_URL: "postgresql://user:password@localhost:5432/pulseroute",
  LOG_LEVEL: "silent",
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
    });

    expect(typeof config.port).toBe("number");
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
});
