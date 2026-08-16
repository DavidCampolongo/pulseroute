import { createHmac } from "node:crypto";

export const WEBHOOK_TIMESTAMP_HEADER = "x-pulseroute-timestamp";
export const WEBHOOK_SIGNATURE_HEADER = "x-pulseroute-signature";

export type CreateSimulatorSignatureOptions = {
  secret: string;
  timestamp: string;
  rawBody: string | Uint8Array;
};

export function createSimulatorSignature(
  options: CreateSimulatorSignatureOptions,
): string {
  const bodyBytes =
    typeof options.rawBody === "string"
      ? Buffer.from(options.rawBody, "utf8")
      : options.rawBody;

  return createHmac("sha256", options.secret)
    .update(options.timestamp, "utf8")
    .update(".", "utf8")
    .update(bodyBytes)
    .digest("hex");
}

export function createSignedWebhookHeaders(
  options: CreateSimulatorSignatureOptions,
): Record<string, string> {
  return {
    "content-type": "application/json",
    [WEBHOOK_TIMESTAMP_HEADER]: options.timestamp,
    [WEBHOOK_SIGNATURE_HEADER]: createSimulatorSignature(options),
  };
}
