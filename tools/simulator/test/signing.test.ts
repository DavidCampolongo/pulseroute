import { createHmac } from "node:crypto";

import { describe, expect, it } from "vitest";

import {
  WEBHOOK_SIGNATURE_HEADER,
  WEBHOOK_TIMESTAMP_HEADER,
  createSignedWebhookHeaders,
  createSimulatorSignature,
} from "../src/signing.js";

const secret = "simulator-test-secret-at-least-32-characters";
const timestamp = "1770000000";
const rawBody = '{"fixture":"exact bytes"}';

function independentSignature(body: string): string {
  return createHmac("sha256", secret)
    .update(Buffer.from(`${timestamp}.${body}`, "utf8"))
    .digest("hex");
}

describe("simulator webhook signing", () => {
  it("signs timestamp dot exact raw UTF-8 bytes", () => {
    expect(
      createSimulatorSignature({
        secret,
        timestamp,
        rawBody,
      }),
    ).toBe(independentSignature(rawBody));
  });

  it("changes the signature when insignificant JSON bytes change", () => {
    const compactSignature = createSimulatorSignature({
      secret,
      timestamp,
      rawBody,
    });
    const spacedBody = '{ "fixture": "exact bytes" }';

    expect(
      createSimulatorSignature({
        secret,
        timestamp,
        rawBody: spacedBody,
      }),
    ).toBe(independentSignature(spacedBody));
    expect(compactSignature).not.toBe(independentSignature(spacedBody));
  });

  it("produces only the public JSON and PulseRoute signing headers", () => {
    expect(
      createSignedWebhookHeaders({
        secret,
        timestamp,
        rawBody,
      }),
    ).toEqual({
      "content-type": "application/json",
      [WEBHOOK_TIMESTAMP_HEADER]: timestamp,
      [WEBHOOK_SIGNATURE_HEADER]: independentSignature(rawBody),
    });
  });
});
