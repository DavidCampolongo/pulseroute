import { Buffer } from "node:buffer";
import { createHmac } from "node:crypto";

import { EVENT_TYPES } from "@pulseroute/shared";
import { z } from "zod";

export const OUTBOUND_WEBHOOK_TIMESTAMP_HEADER = "x-pulseroute-timestamp";
export const OUTBOUND_WEBHOOK_SIGNATURE_HEADER = "x-pulseroute-signature";
export const OUTBOUND_WEBHOOK_EVENT_ID_HEADER = "x-pulseroute-event-id";

const MAX_SAFE_ERROR_MESSAGE_LENGTH = 500;

const assignedEventPayloadSchema = z.object({
  organizationId: z.uuid(),
  serviceRequestId: z.uuid(),
  operatorId: z.uuid(),
  assignmentId: z.uuid(),
  routingDecisionId: z.uuid(),
  scoringVersion: z.string().trim().min(1).max(100),
  correlationId: z.string().trim().min(1).max(200),
});

export type AssignedEventPayload = z.infer<typeof assignedEventPayloadSchema>;

export type OutboundWebhookEnvelope = {
  eventId: string;
  type: typeof EVENT_TYPES.serviceRequestAssigned;
  createdAt: string;
  data: AssignedEventPayload;
};

export type SerializedOutboundWebhook = {
  envelope: OutboundWebhookEnvelope;
  body: Buffer;
};

export type OutboundWebhookResult =
  | {
      outcome: "delivered";
      httpStatus: number;
      durationMs: number;
    }
  | {
      outcome: "http_failure";
      httpStatus: number;
      durationMs: number;
      error: {
        code: "HTTP_ERROR";
        message: string;
      };
    }
  | {
      outcome: "timeout";
      httpStatus: null;
      durationMs: number;
      error: {
        code: "TIMEOUT";
        name: string;
        message: string;
      };
    }
  | {
      outcome: "network_failure";
      httpStatus: null;
      durationMs: number;
      error: {
        code: "NETWORK_ERROR";
        name: string;
        message: string;
      };
    };

export type SendOutboundWebhookOptions = {
  url: string;
  secret: string;
  timeoutMs: number;
  timestamp: string;
  eventId: string;
  body: Uint8Array;
  fetchImplementation?: typeof fetch;
  monotonicNow?: () => number;
};

function assertUuid(value: string, name: string): void {
  if (!z.uuid().safeParse(value).success) {
    throw new Error(`${name} must be a UUID`);
  }
}

function assertOutboundSecret(secret: string): void {
  if (secret.trim().length === 0) {
    throw new Error("Outbound webhook secret must not be blank");
  }

  if (secret !== secret.trim()) {
    throw new Error(
      "Outbound webhook secret must not have leading or trailing whitespace",
    );
  }

  if (secret.length < 32) {
    throw new Error("Outbound webhook secret must be at least 32 characters");
  }
}

function assertTimeout(timeoutMs: number): void {
  if (!Number.isInteger(timeoutMs) || timeoutMs < 1) {
    throw new Error("Outbound webhook timeoutMs must be a positive integer");
  }
}

function assertTimestamp(timestamp: string): void {
  if (!/^(0|[1-9]\d*)$/.test(timestamp)) {
    throw new Error("Outbound webhook timestamp must be epoch seconds");
  }
}

function assertOutboundUrl(value: string): void {
  let url: URL;

  try {
    url = new URL(value);
  } catch {
    throw new Error("Outbound webhook URL must be a valid URL");
  }

  if (url.protocol !== "http:" && url.protocol !== "https:") {
    throw new Error("Outbound webhook URL must use HTTP or HTTPS");
  }

  if (url.username || url.password) {
    throw new Error(
      "Outbound webhook URL must not include embedded credentials",
    );
  }
}

function normalizeError(error: unknown): { name: string; message: string } {
  const name = error instanceof Error ? error.name : "Error";
  const message =
    error instanceof Error
      ? error.message
      : typeof error === "string"
        ? error
        : "Unknown outbound webhook failure";

  return {
    name: name.slice(0, 100),
    message: message.slice(0, MAX_SAFE_ERROR_MESSAGE_LENGTH),
  };
}

export function serializeAssignedOutboundWebhook(options: {
  eventId: string;
  createdAt: Date;
  payload: unknown;
}): SerializedOutboundWebhook {
  assertUuid(options.eventId, "eventId");

  if (Number.isNaN(options.createdAt.getTime())) {
    throw new Error("createdAt must be a valid Date");
  }

  const payloadResult = assignedEventPayloadSchema.safeParse(options.payload);

  if (!payloadResult.success) {
    throw new Error("Assigned OutboxEvent payload is invalid", {
      cause: payloadResult.error,
    });
  }

  const envelope: OutboundWebhookEnvelope = {
    eventId: options.eventId,
    type: EVENT_TYPES.serviceRequestAssigned,
    createdAt: options.createdAt.toISOString(),
    data: payloadResult.data,
  };

  return {
    envelope,
    body: Buffer.from(JSON.stringify(envelope), "utf8"),
  };
}

export function createOutboundWebhookTimestamp(now: Date): string {
  if (Number.isNaN(now.getTime())) {
    throw new Error("now must be a valid Date");
  }

  return Math.floor(now.getTime() / 1_000).toString();
}

export function createOutboundWebhookSignature(options: {
  secret: string;
  timestamp: string;
  body: Uint8Array;
}): string {
  assertOutboundSecret(options.secret);
  assertTimestamp(options.timestamp);

  return createHmac("sha256", options.secret)
    .update(options.timestamp, "utf8")
    .update(".", "utf8")
    .update(options.body)
    .digest("hex");
}

export async function sendOutboundWebhook(
  options: SendOutboundWebhookOptions,
): Promise<OutboundWebhookResult> {
  assertOutboundUrl(options.url);
  assertOutboundSecret(options.secret);
  assertTimeout(options.timeoutMs);
  assertTimestamp(options.timestamp);
  assertUuid(options.eventId, "eventId");

  const fetchImplementation = options.fetchImplementation ?? fetch;
  const monotonicNow = options.monotonicNow ?? (() => performance.now());
  const startedAt = monotonicNow();
  const timeoutSignal = AbortSignal.timeout(options.timeoutMs);
  const signature = createOutboundWebhookSignature({
    secret: options.secret,
    timestamp: options.timestamp,
    body: options.body,
  });

  try {
    const response = await fetchImplementation(options.url, {
      method: "POST",
      redirect: "manual",
      headers: {
        "content-type": "application/json; charset=utf-8",
        [OUTBOUND_WEBHOOK_TIMESTAMP_HEADER]: options.timestamp,
        [OUTBOUND_WEBHOOK_SIGNATURE_HEADER]: signature,
        [OUTBOUND_WEBHOOK_EVENT_ID_HEADER]: options.eventId,
      },
      body: Uint8Array.from(options.body),
      signal: timeoutSignal,
    });

    const durationMs = Math.max(0, Math.round(monotonicNow() - startedAt));

    if (response.body) {
      await response.body.cancel().catch(() => undefined);
    }

    if (response.status >= 200 && response.status < 300) {
      return {
        outcome: "delivered",
        httpStatus: response.status,
        durationMs,
      };
    }

    return {
      outcome: "http_failure",
      httpStatus: response.status,
      durationMs,
      error: {
        code: "HTTP_ERROR",
        message: `Receiver returned HTTP ${response.status}`,
      },
    };
  } catch (error) {
    const durationMs = Math.max(0, Math.round(monotonicNow() - startedAt));
    const normalizedError = normalizeError(error);

    if (timeoutSignal.aborted) {
      return {
        outcome: "timeout",
        httpStatus: null,
        durationMs,
        error: {
          code: "TIMEOUT",
          ...normalizedError,
        },
      };
    }

    return {
      outcome: "network_failure",
      httpStatus: null,
      durationMs,
      error: {
        code: "NETWORK_ERROR",
        ...normalizedError,
      },
    };
  }
}
