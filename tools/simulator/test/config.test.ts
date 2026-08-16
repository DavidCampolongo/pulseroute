import { describe, expect, it } from "vitest";

import {
  DEFAULT_SIMULATOR_TIMEOUT_MS,
  parseSimulatorConfig,
} from "../src/config.js";

const webhookSecret = "simulator-test-secret-at-least-32-characters";

describe("parseSimulatorConfig", () => {
  it("parses and normalizes the complete simulator contract", () => {
    expect(
      parseSimulatorConfig(
        [
          "--base-url",
          "https://pulseroute.example/",
          "--count=5",
          "--duplicates",
          "2",
        ],
        {
          WEBHOOK_SECRET: webhookSecret,
        },
      ),
    ).toEqual({
      baseUrl: "https://pulseroute.example",
      endpoint: "https://pulseroute.example/webhooks/service-requests",
      count: 5,
      duplicates: 2,
      webhookSecret,
      timeoutMs: DEFAULT_SIMULATOR_TIMEOUT_MS,
    });
  });

  it("accepts local HTTP URLs and zero duplicate replays", () => {
    expect(
      parseSimulatorConfig(
        [
          "--duplicates",
          "0",
          "--base-url",
          "http://127.0.0.1:3000",
          "--count",
          "1",
        ],
        {
          WEBHOOK_SECRET: webhookSecret,
        },
      ),
    ).toMatchObject({
      baseUrl: "http://127.0.0.1:3000",
      count: 1,
      duplicates: 0,
    });
  });

  it.each([
    {
      name: "missing options",
      arguments_: ["--base-url", "https://pulseroute.example"],
      expected: "--count: is required",
    },
    {
      name: "unknown options",
      arguments_: [
        "--base-url",
        "https://pulseroute.example",
        "--count",
        "1",
        "--duplicates",
        "0",
        "--scenario",
        "burst",
      ],
      expected: "Unknown simulator option",
    },
    {
      name: "repeated options",
      arguments_: [
        "--base-url",
        "https://pulseroute.example",
        "--count",
        "1",
        "--count",
        "2",
        "--duplicates",
        "0",
      ],
      expected: "--count: may only be provided once",
    },
    {
      name: "zero count",
      arguments_: [
        "--base-url",
        "https://pulseroute.example",
        "--count",
        "0",
        "--duplicates",
        "0",
      ],
      expected: "--count: must be between 1 and 100",
    },
    {
      name: "oversized duplicate count",
      arguments_: [
        "--base-url",
        "https://pulseroute.example",
        "--count",
        "1",
        "--duplicates",
        "101",
      ],
      expected: "--duplicates: must be between 0 and 100",
    },
    {
      name: "fractional count",
      arguments_: [
        "--base-url",
        "https://pulseroute.example",
        "--count",
        "1.5",
        "--duplicates",
        "0",
      ],
      expected: "--count: must be a whole number",
    },
    {
      name: "unsupported protocol",
      arguments_: [
        "--base-url",
        "ftp://pulseroute.example",
        "--count",
        "1",
        "--duplicates",
        "0",
      ],
      expected: "protocol must be HTTP or HTTPS",
    },
    {
      name: "embedded credentials",
      arguments_: [
        "--base-url",
        "https://user:password@pulseroute.example",
        "--count",
        "1",
        "--duplicates",
        "0",
      ],
      expected: "embedded credentials are not allowed",
    },
    {
      name: "application path",
      arguments_: [
        "--base-url",
        "https://pulseroute.example/api",
        "--count",
        "1",
        "--duplicates",
        "0",
      ],
      expected: "must not include an application path",
    },
    {
      name: "query string",
      arguments_: [
        "--base-url",
        "https://pulseroute.example?secret=value",
        "--count",
        "1",
        "--duplicates",
        "0",
      ],
      expected: "query strings and fragments are not allowed",
    },
  ])("rejects $name", ({ arguments_, expected }) => {
    expect(() =>
      parseSimulatorConfig(arguments_, {
        WEBHOOK_SECRET: webhookSecret,
      }),
    ).toThrow(expected);
  });

  it("requires the inbound signing secret without accepting a CLI substitute", () => {
    const arguments_ = [
      "--base-url",
      "https://pulseroute.example",
      "--count",
      "1",
      "--duplicates",
      "0",
    ];

    expect(() => parseSimulatorConfig(arguments_, {})).toThrow(
      "WEBHOOK_SECRET: is required",
    );

    expect(() =>
      parseSimulatorConfig(arguments_, {
        WEBHOOK_SECRET: "too-short",
      }),
    ).toThrow("WEBHOOK_SECRET: must be at least 32 characters");
  });

  it("never includes raw unknown argument values in diagnostics", () => {
    const sentinel = "never-print-this-sentinel-secret-value";
    let diagnostic = "";

    try {
      parseSimulatorConfig(
        [
          "--base-url",
          "https://pulseroute.example",
          "--count",
          "1",
          "--duplicates",
          "0",
          "--webhook-secret",
          sentinel,
        ],
        { WEBHOOK_SECRET: webhookSecret },
      );
    } catch (error) {
      diagnostic = error instanceof Error ? error.message : String(error);
    }

    expect(diagnostic).toContain("Unknown simulator option");
    expect(diagnostic).toContain("Unexpected positional simulator argument");
    expect(diagnostic).not.toContain(sentinel);
  });
});
