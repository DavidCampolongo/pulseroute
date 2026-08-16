import { describe, expect, it } from "vitest";

import { parseWorkerConfig } from "../src/config.js";

const validEnvironment = {
  NODE_ENV: "test",
  DATABASE_URL:
    "postgresql://pulseroute:password@127.0.0.1:5432/pulseroute_test",
  REDIS_URL: "redis://127.0.0.1:6379",
  LOG_LEVEL: "info",
  WEBHOOK_DELIVERY_URL: "https://receiver.example.test/webhooks",
  OUTBOUND_WEBHOOK_SECRET: "outbound-test-secret-at-least-32-characters",
};

describe("parseWorkerConfig", () => {
  it("parses valid worker configuration with scoring faults disabled by default", () => {
    expect(parseWorkerConfig(validEnvironment)).toEqual({
      nodeEnv: "test",
      databaseUrl:
        "postgresql://pulseroute:password@127.0.0.1:5432/pulseroute_test",
      redisUrl: "redis://127.0.0.1:6379",
      logLevel: "info",
      faultInjectScoring: false,
      webhookDeliveryUrl: "https://receiver.example.test/webhooks",
      outboundWebhookSecret: "outbound-test-secret-at-least-32-characters",
      webhookDeliveryTimeoutMs: 5_000,
    });
  });

  it("enables scoring fault injection explicitly", () => {
    expect(
      parseWorkerConfig({
        ...validEnvironment,
        FAULT_INJECT_SCORING: "true",
      }).faultInjectScoring,
    ).toBe(true);
  });

  it("rejects an invalid scoring fault flag", () => {
    expect(() =>
      parseWorkerConfig({
        ...validEnvironment,
        FAULT_INJECT_SCORING: "yes",
      }),
    ).toThrow("FAULT_INJECT_SCORING");
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

  it("parses an explicit bounded delivery timeout", () => {
    expect(
      parseWorkerConfig({
        ...validEnvironment,
        WEBHOOK_DELIVERY_TIMEOUT_MS: "2750",
      }).webhookDeliveryTimeoutMs,
    ).toBe(2_750);
  });

  it("rejects a missing or non-HTTP delivery URL", () => {
    expect(() =>
      parseWorkerConfig({
        ...validEnvironment,
        WEBHOOK_DELIVERY_URL: undefined,
      }),
    ).toThrow("WEBHOOK_DELIVERY_URL");

    expect(() =>
      parseWorkerConfig({
        ...validEnvironment,
        WEBHOOK_DELIVERY_URL: "ftp://receiver.example.test/webhooks",
      }),
    ).toThrow("WEBHOOK_DELIVERY_URL must be a valid HTTP or HTTPS URL");
  });

  it("requires HTTPS in production while allowing loopback HTTP in test", () => {
    const loopbackUrl = "http://127.0.0.1:3100/webhooks";

    expect(
      parseWorkerConfig({
        ...validEnvironment,
        NODE_ENV: "test",
        WEBHOOK_DELIVERY_URL: loopbackUrl,
      }).webhookDeliveryUrl,
    ).toBe(loopbackUrl);

    expect(() =>
      parseWorkerConfig({
        ...validEnvironment,
        NODE_ENV: "production",
        WEBHOOK_DELIVERY_URL: loopbackUrl,
      }),
    ).toThrow("WEBHOOK_DELIVERY_URL must use HTTPS in production");

    expect(
      parseWorkerConfig({
        ...validEnvironment,
        NODE_ENV: "production",
      }).nodeEnv,
    ).toBe("production");

    expect(() =>
      parseWorkerConfig({
        ...validEnvironment,
        WEBHOOK_DELIVERY_URL: "http://receiver.example.test/webhooks",
      }),
    ).toThrow("WEBHOOK_DELIVERY_URL may use HTTP only for a loopback receiver");

    for (const allowedLoopbackUrl of [
      "http://localhost:3100/webhooks",
      "http://127.0.0.1:3100/webhooks",
      "http://[::1]:3100/webhooks",
    ]) {
      expect(
        parseWorkerConfig({
          ...validEnvironment,
          WEBHOOK_DELIVERY_URL: allowedLoopbackUrl,
        }).webhookDeliveryUrl,
      ).toBe(allowedLoopbackUrl);
    }
  });

  it("rejects embedded delivery URL credentials without echoing them", () => {
    for (const credentialUrl of [
      "https://audit-user@receiver.example.test/webhooks",
      "https://audit-user:audit-password@receiver.example.test/webhooks",
    ]) {
      let thrown: unknown;

      try {
        parseWorkerConfig({
          ...validEnvironment,
          WEBHOOK_DELIVERY_URL: credentialUrl,
        });
      } catch (error) {
        thrown = error;
      }

      expect(thrown).toBeInstanceOf(Error);
      expect((thrown as Error).message).toContain(
        "WEBHOOK_DELIVERY_URL must not include embedded credentials",
      );
      expect((thrown as Error).message).not.toContain("audit-user");
      expect((thrown as Error).message).not.toContain("audit-password");
    }
  });

  it("requires a strong distinct outbound secret", () => {
    expect(() =>
      parseWorkerConfig({
        ...validEnvironment,
        OUTBOUND_WEBHOOK_SECRET: undefined,
      }),
    ).toThrow("OUTBOUND_WEBHOOK_SECRET");

    expect(() =>
      parseWorkerConfig({
        ...validEnvironment,
        OUTBOUND_WEBHOOK_SECRET: "too-short",
      }),
    ).toThrow("OUTBOUND_WEBHOOK_SECRET must be at least 32 characters");
  });

  it.each([
    ["leading whitespace", ` ${validEnvironment.OUTBOUND_WEBHOOK_SECRET}`],
    ["trailing whitespace", `${validEnvironment.OUTBOUND_WEBHOOK_SECRET} `],
    ["blank whitespace", " ".repeat(32)],
  ])("rejects an outbound secret with %s", (_description, secret) => {
    expect(() =>
      parseWorkerConfig({
        ...validEnvironment,
        OUTBOUND_WEBHOOK_SECRET: secret,
      }),
    ).toThrow(/OUTBOUND_WEBHOOK_SECRET.*(?:whitespace|blank)/s);
  });

  it("requires the outbound secret to differ from the inbound secret", () => {
    expect(() =>
      parseWorkerConfig({
        ...validEnvironment,
        WEBHOOK_SECRET: validEnvironment.OUTBOUND_WEBHOOK_SECRET,
      }),
    ).toThrow("OUTBOUND_WEBHOOK_SECRET must be distinct from WEBHOOK_SECRET");
  });

  it("rejects invalid or excessive delivery timeouts", () => {
    for (const timeout of ["0", "1.5", "120001", "not-a-number"]) {
      expect(() =>
        parseWorkerConfig({
          ...validEnvironment,
          WEBHOOK_DELIVERY_TIMEOUT_MS: timeout,
        }),
      ).toThrow("WEBHOOK_DELIVERY_TIMEOUT_MS");
    }
  });
});
