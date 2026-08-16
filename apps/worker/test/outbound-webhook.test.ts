import { createHmac, randomUUID } from "node:crypto";
import { createServer } from "node:http";

import {
  startFakeReceiver,
  type RunningFakeReceiver,
} from "@pulseroute/fake-receiver";
import { afterEach, describe, expect, it } from "vitest";

import {
  createOutboundWebhookSignature,
  createOutboundWebhookTimestamp,
  sendOutboundWebhook,
  serializeAssignedOutboundWebhook,
} from "../src/outbound-webhook.js";

const SECRET = "outbound-webhook-unit-secret-is-at-least-32-characters";
const CREATED_AT = new Date("2026-08-15T12:34:56.789Z");
const NOW = new Date("2026-08-15T12:35:00.999Z");

let receiver: RunningFakeReceiver | undefined;

function createSerializedFixture() {
  return serializeAssignedOutboundWebhook({
    eventId: "91000000-0000-4000-8000-000000000001",
    createdAt: CREATED_AT,
    payload: {
      organizationId: "91000000-0000-4000-8000-000000000002",
      serviceRequestId: "91000000-0000-4000-8000-000000000003",
      operatorId: "91000000-0000-4000-8000-000000000004",
      assignmentId: "91000000-0000-4000-8000-000000000005",
      routingDecisionId: "91000000-0000-4000-8000-000000000006",
      scoringVersion: "pulseroute-scoring-v1",
      correlationId: "outbound-webhook-unit-correlation",
    },
  });
}

afterEach(async () => {
  await receiver?.close();
  receiver = undefined;
});

describe("outbound webhook", () => {
  it("serializes one stable envelope and signs the exact UTF-8 bytes", () => {
    const serialized = createSerializedFixture();
    const timestamp = createOutboundWebhookTimestamp(NOW);
    const signature = createOutboundWebhookSignature({
      secret: SECRET,
      timestamp,
      body: serialized.body,
    });
    const independentlySerializedBody = JSON.stringify({
      eventId: "91000000-0000-4000-8000-000000000001",
      type: "service_request.assigned",
      createdAt: CREATED_AT.toISOString(),
      data: {
        organizationId: "91000000-0000-4000-8000-000000000002",
        serviceRequestId: "91000000-0000-4000-8000-000000000003",
        operatorId: "91000000-0000-4000-8000-000000000004",
        assignmentId: "91000000-0000-4000-8000-000000000005",
        routingDecisionId: "91000000-0000-4000-8000-000000000006",
        scoringVersion: "pulseroute-scoring-v1",
        correlationId: "outbound-webhook-unit-correlation",
      },
    });
    const independentSignature = createHmac("sha256", SECRET)
      .update(timestamp, "utf8")
      .update(".", "utf8")
      .update(Buffer.from(independentlySerializedBody, "utf8"))
      .digest("hex");

    expect(serialized.body.toString("utf8")).toBe(independentlySerializedBody);
    expect(timestamp).toBe("1786797300");
    expect(signature).toBe(independentSignature);
    expect(signature).toMatch(/^[0-9a-f]{64}$/);

    const tamperedBody = Buffer.from(serialized.body);

    tamperedBody[tamperedBody.length - 2] =
      tamperedBody[tamperedBody.length - 2]! ^ 1;

    expect(
      createOutboundWebhookSignature({
        secret: SECRET,
        timestamp,
        body: tamperedBody,
      }),
    ).not.toBe(signature);
    expect(
      createOutboundWebhookSignature({
        secret: `${SECRET}-wrong`,
        timestamp,
        body: serialized.body,
      }),
    ).not.toBe(signature);
  });

  it.each([
    ["leading whitespace", ` ${SECRET}`],
    ["trailing whitespace", `${SECRET} `],
    ["blank whitespace", " ".repeat(32)],
  ])("rejects a direct outbound secret with %s", (_description, secret) => {
    const serialized = createSerializedFixture();

    expect(() =>
      createOutboundWebhookSignature({
        secret,
        timestamp: createOutboundWebhookTimestamp(NOW),
        body: serialized.body,
      }),
    ).toThrow(/(?:whitespace|blank)/);
  });

  it("rejects embedded target credentials before starting HTTP", async () => {
    const serialized = createSerializedFixture();
    let thrown: unknown;

    try {
      await sendOutboundWebhook({
        url: "https://audit-user:audit-password@receiver.example.test/webhooks",
        secret: SECRET,
        timeoutMs: 1_000,
        timestamp: createOutboundWebhookTimestamp(NOW),
        eventId: serialized.envelope.eventId,
        body: serialized.body,
      });
    } catch (error) {
      thrown = error;
    }

    expect(thrown).toBeInstanceOf(Error);
    expect((thrown as Error).message).toBe(
      "Outbound webhook URL must not include embedded credentials",
    );
    expect((thrown as Error).message).not.toContain("audit-user");
    expect((thrown as Error).message).not.toContain("audit-password");
  });

  it("sends exact signed bytes and classifies real 2xx and 500 responses", async () => {
    receiver = await startFakeReceiver({
      secret: SECRET,
      mode: "success",
    });
    const serialized = createSerializedFixture();
    const timestamp = createOutboundWebhookTimestamp(NOW);
    let clock = 10;

    const success = await sendOutboundWebhook({
      url: receiver.url,
      secret: SECRET,
      timeoutMs: 1_000,
      timestamp,
      eventId: serialized.envelope.eventId,
      body: serialized.body,
      monotonicNow: () => {
        clock += 5;

        return clock;
      },
    });

    expect(success).toEqual({
      outcome: "delivered",
      httpStatus: 200,
      durationMs: 5,
    });
    expect(receiver.getRequests()[0]).toMatchObject({
      signatureAccepted: true,
      statusCode: 200,
      outcome: "success",
      rawBody: serialized.body,
    });

    receiver.setMode("failure");

    const failure = await sendOutboundWebhook({
      url: receiver.url,
      secret: SECRET,
      timeoutMs: 1_000,
      timestamp,
      eventId: serialized.envelope.eventId,
      body: serialized.body,
    });

    expect(failure).toMatchObject({
      outcome: "http_failure",
      httpStatus: 500,
      error: {
        code: "HTTP_ERROR",
        message: "Receiver returned HTTP 500",
      },
    });
    expect(receiver.getRequests()[1]).toMatchObject({
      signatureAccepted: true,
      statusCode: 500,
      outcome: "failure",
    });
  });

  it("does not follow a cross-origin HTTP 307 redirect", async () => {
    const redirectTarget = await startFakeReceiver({
      secret: SECRET,
    });
    receiver = redirectTarget;
    const redirectServer = createServer((request, response) => {
      request.resume();
      response.writeHead(307, {
        location: redirectTarget.url,
      });
      response.end();
    });

    await new Promise<void>((resolve, reject) => {
      redirectServer.once("error", reject);
      redirectServer.listen(0, "127.0.0.1", () => resolve());
    });

    const address = redirectServer.address();

    if (address === null || typeof address === "string") {
      redirectServer.close();
      throw new Error("Redirect test server did not bind to a TCP port");
    }

    try {
      const serialized = createSerializedFixture();
      const result = await sendOutboundWebhook({
        url: `http://127.0.0.1:${address.port}/redirect`,
        secret: SECRET,
        timeoutMs: 1_000,
        timestamp: createOutboundWebhookTimestamp(NOW),
        eventId: serialized.envelope.eventId,
        body: serialized.body,
      });

      expect(result).toMatchObject({
        outcome: "http_failure",
        httpStatus: 307,
        error: {
          code: "HTTP_ERROR",
          message: "Receiver returned HTTP 307",
        },
      });
      expect(redirectTarget.getRequests()).toHaveLength(0);
    } finally {
      await new Promise<void>((resolve, reject) => {
        redirectServer.close((error) => {
          if (error) {
            reject(error);

            return;
          }

          resolve();
        });
        redirectServer.closeAllConnections();
      });
    }
  });

  it("aborts a real slow receiver at the configured timeout", async () => {
    receiver = await startFakeReceiver({
      secret: SECRET,
      mode: "timeout",
      delayMs: 250,
    });
    const serialized = createSerializedFixture();
    const startedAt = Date.now();

    const result = await sendOutboundWebhook({
      url: receiver.url,
      secret: SECRET,
      timeoutMs: 25,
      timestamp: createOutboundWebhookTimestamp(NOW),
      eventId: serialized.envelope.eventId,
      body: serialized.body,
    });

    expect(result).toMatchObject({
      outcome: "timeout",
      httpStatus: null,
      error: {
        code: "TIMEOUT",
      },
    });
    expect(Date.now() - startedAt).toBeLessThan(1_000);

    await new Promise((resolve) => {
      setTimeout(resolve, 25);
    });

    expect(receiver.getRequests()[0]).toMatchObject({
      signatureAccepted: true,
      outcome: "client_aborted",
    });
  });

  it("rejects malformed durable assignment payloads before HTTP", () => {
    expect(() =>
      serializeAssignedOutboundWebhook({
        eventId: randomUUID(),
        createdAt: CREATED_AT,
        payload: {
          organizationId: randomUUID(),
          serviceRequestId: randomUUID(),
          correlationId: "missing assignment evidence",
        },
      }),
    ).toThrow("Assigned OutboxEvent payload is invalid");
  });
});
