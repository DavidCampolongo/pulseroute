import { describe, expect, it } from "vitest";

import { parseWorkerConfig } from "../src/config.js";

const validEnvironment = {
  NODE_ENV: "test",
  DATABASE_URL:
    "postgresql://pulseroute:password@127.0.0.1:5432/pulseroute_test",
  REDIS_URL: "redis://127.0.0.1:6379",
  LOG_LEVEL: "info",
};

describe("parseWorkerConfig", () => {
  it("parses valid worker configuration", () => {
    expect(parseWorkerConfig(validEnvironment)).toEqual({
      nodeEnv: "test",
      databaseUrl:
        "postgresql://pulseroute:password@127.0.0.1:5432/pulseroute_test",
      redisUrl: "redis://127.0.0.1:6379",
      logLevel: "info",
    });
  });

  it("rejects a missing DATABASE_URL", () => {
    expect(() =>
      parseWorkerConfig({
        ...validEnvironment,
        DATABASE_URL: undefined,
      }),
    ).toThrow("DATABASE_URL");
  });

  it("rejects a non-PostgreSQL DATABASE_URL", () => {
    expect(() =>
      parseWorkerConfig({
        ...validEnvironment,
        DATABASE_URL: "https://example.com/database",
      }),
    ).toThrow("DATABASE_URL must be a valid PostgreSQL URL");
  });

  it("rejects a missing REDIS_URL", () => {
    expect(() =>
      parseWorkerConfig({
        ...validEnvironment,
        REDIS_URL: undefined,
      }),
    ).toThrow("REDIS_URL");
  });

  it("rejects a non-Redis REDIS_URL", () => {
    expect(() =>
      parseWorkerConfig({
        ...validEnvironment,
        REDIS_URL: "https://example.com",
      }),
    ).toThrow("REDIS_URL must be a valid Redis URL");
  });

  it("accepts a TLS Redis URL", () => {
    expect(
      parseWorkerConfig({
        ...validEnvironment,
        REDIS_URL: "rediss://redis.example.com:6379",
      }).redisUrl,
    ).toBe("rediss://redis.example.com:6379");
  });

  it("rejects an invalid NODE_ENV", () => {
    expect(() =>
      parseWorkerConfig({
        ...validEnvironment,
        NODE_ENV: "staging",
      }),
    ).toThrow("NODE_ENV");
  });

  it("rejects an invalid LOG_LEVEL", () => {
    expect(() =>
      parseWorkerConfig({
        ...validEnvironment,
        LOG_LEVEL: "verbose",
      }),
    ).toThrow("LOG_LEVEL");
  });
});
