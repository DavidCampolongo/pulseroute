import { describe, expect, it } from "vitest";

import {
  createWebhookSignature,
  verifyWebhookSignature,
} from "../src/webhooks/signature.js";

const secret = "test-webhook-secret-at-least-32-characters";
const wrongSecret = "wrong-webhook-secret-at-least-32-characters";

const nowSeconds = 2_000_000_000;
const toleranceSeconds = 300;

const rawBody = Buffer.from('{"externalId":"phase-5-signature-test"}', "utf8");

function sign(
  timestamp: string,
  body: Buffer = rawBody,
  signingSecret: string = secret,
): string {
  return createWebhookSignature({
    secret: signingSecret,
    timestamp,
    rawBody: body,
  });
}

describe("webhook signature verification", () => {
  it("accepts a correct signature", () => {
    const timestamp = String(nowSeconds);

    const result = verifyWebhookSignature({
      secret,
      timestamp,
      signature: sign(timestamp),
      rawBody,
      toleranceSeconds,
      nowSeconds,
    });

    expect(result).toEqual({
      ok: true,
      timestampSeconds: nowSeconds,
    });
  });

  it("rejects an incorrect equal-length signature", () => {
    const timestamp = String(nowSeconds);

    const result = verifyWebhookSignature({
      secret,
      timestamp,
      signature: "0".repeat(64),
      rawBody,
      toleranceSeconds,
      nowSeconds,
    });

    expect(result).toEqual({
      ok: false,
      reason: "SIGNATURE_MISMATCH",
    });
  });

  it("rejects changed raw body bytes", () => {
    const timestamp = String(nowSeconds);

    const originalSignature = sign(timestamp);

    const changedBody = Buffer.from(
      '{"externalId":"phase-5-signature-tesT"}',
      "utf8",
    );

    const result = verifyWebhookSignature({
      secret,
      timestamp,
      signature: originalSignature,
      rawBody: changedBody,
      toleranceSeconds,
      nowSeconds,
    });

    expect(result).toEqual({
      ok: false,
      reason: "SIGNATURE_MISMATCH",
    });
  });

  it("rejects a signature created with the wrong secret", () => {
    const timestamp = String(nowSeconds);

    const result = verifyWebhookSignature({
      secret,
      timestamp,
      signature: sign(timestamp, rawBody, wrongSecret),
      rawBody,
      toleranceSeconds,
      nowSeconds,
    });

    expect(result).toEqual({
      ok: false,
      reason: "SIGNATURE_MISMATCH",
    });
  });

  it("rejects a stale timestamp even with a valid signature", () => {
    const timestamp = String(nowSeconds - toleranceSeconds - 1);

    const result = verifyWebhookSignature({
      secret,
      timestamp,
      signature: sign(timestamp),
      rawBody,
      toleranceSeconds,
      nowSeconds,
    });

    expect(result).toEqual({
      ok: false,
      reason: "STALE_TIMESTAMP",
    });
  });

  it("rejects an excessively future timestamp even with a valid signature", () => {
    const timestamp = String(nowSeconds + toleranceSeconds + 1);

    const result = verifyWebhookSignature({
      secret,
      timestamp,
      signature: sign(timestamp),
      rawBody,
      toleranceSeconds,
      nowSeconds,
    });

    expect(result).toEqual({
      ok: false,
      reason: "FUTURE_TIMESTAMP",
    });
  });

  it("rejects a malformed timestamp", () => {
    const result = verifyWebhookSignature({
      secret,
      timestamp: "not-a-timestamp",
      signature: "0".repeat(64),
      rawBody,
      toleranceSeconds,
      nowSeconds,
    });

    expect(result).toEqual({
      ok: false,
      reason: "MALFORMED_TIMESTAMP",
    });
  });

  it("rejects a missing timestamp", () => {
    const result = verifyWebhookSignature({
      secret,
      timestamp: undefined,
      signature: "0".repeat(64),
      rawBody,
      toleranceSeconds,
      nowSeconds,
    });

    expect(result).toEqual({
      ok: false,
      reason: "MISSING_TIMESTAMP",
    });
  });

  it("rejects a missing signature", () => {
    const timestamp = String(nowSeconds);

    const result = verifyWebhookSignature({
      secret,
      timestamp,
      signature: undefined,
      rawBody,
      toleranceSeconds,
      nowSeconds,
    });

    expect(result).toEqual({
      ok: false,
      reason: "MISSING_SIGNATURE",
    });
  });

  it("rejects a wrong-length signature without throwing", () => {
    const timestamp = String(nowSeconds);

    expect(() =>
      verifyWebhookSignature({
        secret,
        timestamp,
        signature: "ab".repeat(31),
        rawBody,
        toleranceSeconds,
        nowSeconds,
      }),
    ).not.toThrow();

    const result = verifyWebhookSignature({
      secret,
      timestamp,
      signature: "ab".repeat(31),
      rawBody,
      toleranceSeconds,
      nowSeconds,
    });

    expect(result).toEqual({
      ok: false,
      reason: "MALFORMED_SIGNATURE",
    });
  });

  it("rejects a non-hexadecimal signature", () => {
    const timestamp = String(nowSeconds);

    const result = verifyWebhookSignature({
      secret,
      timestamp,
      signature: "z".repeat(64),
      rawBody,
      toleranceSeconds,
      nowSeconds,
    });

    expect(result).toEqual({
      ok: false,
      reason: "MALFORMED_SIGNATURE",
    });
  });

  it("rejects a changed timestamp when the old signature is reused", () => {
    const originalTimestamp = String(nowSeconds - 10);
    const changedTimestamp = String(nowSeconds);

    const originalSignature = sign(originalTimestamp);

    const result = verifyWebhookSignature({
      secret,
      timestamp: changedTimestamp,
      signature: originalSignature,
      rawBody,
      toleranceSeconds,
      nowSeconds,
    });

    expect(result).toEqual({
      ok: false,
      reason: "SIGNATURE_MISMATCH",
    });
  });

  it("accepts the same body with a new timestamp when the signature is recomputed", () => {
    const firstTimestamp = String(nowSeconds - 20);
    const secondTimestamp = String(nowSeconds - 5);

    const firstSignature = sign(firstTimestamp);
    const secondSignature = sign(secondTimestamp);

    expect(secondSignature).not.toBe(firstSignature);

    const result = verifyWebhookSignature({
      secret,
      timestamp: secondTimestamp,
      signature: secondSignature,
      rawBody,
      toleranceSeconds,
      nowSeconds,
    });

    expect(result).toEqual({
      ok: true,
      timestampSeconds: nowSeconds - 5,
    });
  });
});
