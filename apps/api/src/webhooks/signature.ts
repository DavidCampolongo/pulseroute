import { createHmac, timingSafeEqual } from "node:crypto";

export type WebhookVerificationFailureReason =
  | "MISSING_TIMESTAMP"
  | "MALFORMED_TIMESTAMP"
  | "STALE_TIMESTAMP"
  | "FUTURE_TIMESTAMP"
  | "MISSING_SIGNATURE"
  | "MALFORMED_SIGNATURE"
  | "SIGNATURE_MISMATCH";

export type WebhookVerificationResult =
  | {
      ok: true;
      timestampSeconds: number;
    }
  | {
      ok: false;
      reason: WebhookVerificationFailureReason;
    };

type CreateWebhookSignatureInput = {
  secret: string;
  timestamp: string;
  rawBody: Buffer;
};

type VerifyWebhookSignatureInput = {
  secret: string;
  timestamp: string | undefined;
  signature: string | undefined;
  rawBody: Buffer;
  toleranceSeconds: number;
  nowSeconds: number;
};

const hexadecimalPattern = /^[0-9a-f]+$/;
const timestampPattern = /^(0|[1-9]\d*)$/;

export function createWebhookSignature({
  secret,
  timestamp,
  rawBody,
}: CreateWebhookSignatureInput): string {
  return createHmac("sha256", secret)
    .update(timestamp, "utf8")
    .update(".", "utf8")
    .update(rawBody)
    .digest("hex");
}

export function verifyWebhookSignature({
  secret,
  timestamp,
  signature,
  rawBody,
  toleranceSeconds,
  nowSeconds,
}: VerifyWebhookSignatureInput): WebhookVerificationResult {
  if (timestamp === undefined) {
    return {
      ok: false,
      reason: "MISSING_TIMESTAMP",
    };
  }

  if (!timestampPattern.test(timestamp)) {
    return {
      ok: false,
      reason: "MALFORMED_TIMESTAMP",
    };
  }

  const timestampSeconds = Number(timestamp);

  if (!Number.isSafeInteger(timestampSeconds) || timestampSeconds <= 0) {
    return {
      ok: false,
      reason: "MALFORMED_TIMESTAMP",
    };
  }

  const ageSeconds = nowSeconds - timestampSeconds;

  if (ageSeconds > toleranceSeconds) {
    return {
      ok: false,
      reason: "STALE_TIMESTAMP",
    };
  }

  if (ageSeconds < -toleranceSeconds) {
    return {
      ok: false,
      reason: "FUTURE_TIMESTAMP",
    };
  }

  if (signature === undefined) {
    return {
      ok: false,
      reason: "MISSING_SIGNATURE",
    };
  }

  if (
    signature.length === 0 ||
    signature.length % 2 !== 0 ||
    !hexadecimalPattern.test(signature)
  ) {
    return {
      ok: false,
      reason: "MALFORMED_SIGNATURE",
    };
  }

  const expectedSignature = Buffer.from(
    createWebhookSignature({
      secret,
      timestamp,
      rawBody,
    }),
    "hex",
  );

  const receivedSignature = Buffer.from(signature, "hex");

  if (expectedSignature.length !== receivedSignature.length) {
    return {
      ok: false,
      reason: "MALFORMED_SIGNATURE",
    };
  }

  if (!timingSafeEqual(expectedSignature, receivedSignature)) {
    return {
      ok: false,
      reason: "SIGNATURE_MISMATCH",
    };
  }

  return {
    ok: true,
    timestampSeconds,
  };
}
