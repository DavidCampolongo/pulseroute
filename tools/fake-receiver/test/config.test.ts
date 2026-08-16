import { describe, expect, it } from "vitest";

import {
  DEFAULT_RECEIVER_DELAY_MS,
  DEFAULT_RECEIVER_HOST,
  DEFAULT_RECEIVER_PORT,
  PRODUCTION_RECEIVER_HOST,
  parseFakeReceiverConfig,
} from "../src/config.js";

const validSecret = "test-outbound-secret-at-least-32-characters";

describe("parseFakeReceiverConfig", () => {
  it("applies safe local defaults", () => {
    expect(
      parseFakeReceiverConfig({
        OUTBOUND_WEBHOOK_SECRET: validSecret,
      }),
    ).toEqual({
      host: DEFAULT_RECEIVER_HOST,
      port: DEFAULT_RECEIVER_PORT,
      outboundWebhookSecret: validSecret,
      mode: "success",
      delayMs: DEFAULT_RECEIVER_DELAY_MS,
    });
  });

  it("parses each explicit behavior setting", () => {
    expect(
      parseFakeReceiverConfig({
        RECEIVER_PORT: "4310",
        OUTBOUND_WEBHOOK_SECRET: validSecret,
        RECEIVER_MODE: "timeout",
        RECEIVER_DELAY_MS: "125",
      }),
    ).toEqual({
      host: DEFAULT_RECEIVER_HOST,
      port: 4_310,
      outboundWebhookSecret: validSecret,
      mode: "timeout",
      delayMs: 125,
    });
  });

  it("uses Railway PORT and binds all interfaces in production", () => {
    expect(
      parseFakeReceiverConfig({
        NODE_ENV: "production",
        PORT: "8080",
        RECEIVER_PORT: "4310",
        OUTBOUND_WEBHOOK_SECRET: validSecret,
      }),
    ).toMatchObject({
      host: PRODUCTION_RECEIVER_HOST,
      port: 8_080,
    });
  });

  it("rejects a missing or short secret", () => {
    expect(() => parseFakeReceiverConfig({})).toThrow(
      /OUTBOUND_WEBHOOK_SECRET/,
    );

    expect(() =>
      parseFakeReceiverConfig({
        OUTBOUND_WEBHOOK_SECRET: "too-short",
      }),
    ).toThrow(/at least 32 characters/);
  });

  it.each([
    ["leading whitespace", ` ${validSecret}`],
    ["trailing whitespace", `${validSecret} `],
    ["blank whitespace", " ".repeat(32)],
  ])("rejects an outbound secret with %s", (_description, secret) => {
    expect(() =>
      parseFakeReceiverConfig({
        OUTBOUND_WEBHOOK_SECRET: secret,
      }),
    ).toThrow(/OUTBOUND_WEBHOOK_SECRET.*(?:whitespace|blank)/s);
  });

  it.each([
    ["PORT", "0"],
    ["PORT", "65536"],
    ["PORT", "3.5"],
    ["RECEIVER_PORT", "0"],
    ["RECEIVER_PORT", "65536"],
    ["RECEIVER_PORT", "3.5"],
    ["RECEIVER_DELAY_MS", "0"],
    ["RECEIVER_DELAY_MS", "60001"],
    ["RECEIVER_DELAY_MS", "not-a-number"],
  ])("rejects invalid %s value %s", (name, value) => {
    expect(() =>
      parseFakeReceiverConfig({
        OUTBOUND_WEBHOOK_SECRET: validSecret,
        [name]: value,
      }),
    ).toThrow(new RegExp(name));
  });

  it("rejects an unknown receiver mode", () => {
    expect(() =>
      parseFakeReceiverConfig({
        OUTBOUND_WEBHOOK_SECRET: validSecret,
        RECEIVER_MODE: "flaky",
      }),
    ).toThrow(/RECEIVER_MODE/);
  });

  it("reports all invalid settings in one startup error", () => {
    expect(() =>
      parseFakeReceiverConfig({
        RECEIVER_PORT: "invalid",
        OUTBOUND_WEBHOOK_SECRET: "short",
        RECEIVER_MODE: "unknown",
        RECEIVER_DELAY_MS: "-1",
      }),
    ).toThrow(
      /RECEIVER_PORT[\s\S]*OUTBOUND_WEBHOOK_SECRET[\s\S]*RECEIVER_MODE[\s\S]*RECEIVER_DELAY_MS/,
    );
  });
});
