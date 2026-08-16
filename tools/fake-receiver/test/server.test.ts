import { Buffer } from "node:buffer";
import { createHmac } from "node:crypto";

import { afterEach, describe, expect, it } from "vitest";

import {
  MAX_FAKE_RECEIVER_REQUEST_BODY_BYTES,
  MAX_FAKE_RECEIVER_REQUEST_HISTORY,
  OUTBOUND_WEBHOOK_SIGNATURE_HEADER,
  OUTBOUND_WEBHOOK_TIMESTAMP_HEADER,
  startFakeReceiver,
  type RunningFakeReceiver,
} from "../src/server.js";

const receiverSecret = "test-outbound-secret-at-least-32-characters";
const wrongSecret = "wrong-outbound-secret-at-least-32-characters";
const timestamp = "2000000000";

const activeReceivers: RunningFakeReceiver[] = [];

function independentlySign(
  rawBody: Buffer,
  signedTimestamp = timestamp,
  secret = receiverSecret,
): string {
  return createHmac("sha256", secret)
    .update(signedTimestamp, "utf8")
    .update(".", "utf8")
    .update(rawBody)
    .digest("hex");
}

async function createReceiver(
  options: Parameters<typeof startFakeReceiver>[0] = {
    secret: receiverSecret,
  },
): Promise<RunningFakeReceiver> {
  const receiver = await startFakeReceiver(options);

  activeReceivers.push(receiver);

  return receiver;
}

async function postSigned(options: {
  receiver: RunningFakeReceiver;
  rawBody: Buffer;
  headerTimestamp?: string;
  signature?: string;
  includeTimestamp?: boolean;
  includeSignature?: boolean;
  signal?: AbortSignal;
}): Promise<Response> {
  const headerTimestamp = options.headerTimestamp ?? timestamp;
  const headers: Record<string, string> = {
    "content-type": "application/json",
  };

  if (options.includeTimestamp !== false) {
    headers[OUTBOUND_WEBHOOK_TIMESTAMP_HEADER] = headerTimestamp;
  }

  if (options.includeSignature !== false) {
    headers[OUTBOUND_WEBHOOK_SIGNATURE_HEADER] =
      options.signature ?? independentlySign(options.rawBody, headerTimestamp);
  }

  return fetch(options.receiver.url, {
    method: "POST",
    headers,
    body: Uint8Array.from(options.rawBody),
    signal: options.signal,
  });
}

afterEach(async () => {
  await Promise.all(
    activeReceivers.splice(0).map((receiver) => receiver.close()),
  );
});

describe("fake receiver signature verification", () => {
  it.each([
    ["leading whitespace", ` ${receiverSecret}`],
    ["trailing whitespace", `${receiverSecret} `],
    ["blank whitespace", " ".repeat(32)],
  ])(
    "rejects a direct receiver secret with %s",
    async (_description, secret) => {
      await expect(startFakeReceiver({ secret })).rejects.toThrow(
        /(?:whitespace|blank)/,
      );
    },
  );

  it("accepts independently signed exact raw bytes", async () => {
    const receiver = await createReceiver();
    const rawBody = Buffer.from(
      '{ "type": "service_request.assigned", "data": {"id":"a"} }',
      "utf8",
    );

    const response = await postSigned({ receiver, rawBody });

    expect(response.status).toBe(200);

    const requests = receiver.getRequests();

    expect(requests).toHaveLength(1);
    expect(requests[0]).toMatchObject({
      requestNumber: 1,
      timestamp,
      signatureAccepted: true,
      mode: "success",
      statusCode: 200,
      outcome: "success",
    });
    expect(requests[0]?.rawBody.equals(rawBody)).toBe(true);
  });

  it("rejects a one-byte body change", async () => {
    const receiver = await createReceiver();
    const originalBody = Buffer.from('{"value":"original"}', "utf8");
    const changedBody = Buffer.from('{"value":"originaL"}', "utf8");

    const response = await postSigned({
      receiver,
      rawBody: changedBody,
      signature: independentlySign(originalBody),
    });

    expect(response.status).toBe(401);
    expect(receiver.getRequests()[0]).toMatchObject({
      signatureAccepted: false,
      mode: null,
      statusCode: 401,
      outcome: "signature_rejected",
    });
  });

  it("rejects a changed timestamp", async () => {
    const receiver = await createReceiver();
    const rawBody = Buffer.from('{"value":"timestamp"}', "utf8");

    const response = await postSigned({
      receiver,
      rawBody,
      headerTimestamp: "2000000001",
      signature: independentlySign(rawBody, timestamp),
    });

    expect(response.status).toBe(401);
  });

  it("rejects a signature made with the wrong secret", async () => {
    const receiver = await createReceiver();
    const rawBody = Buffer.from('{"value":"secret"}', "utf8");

    const response = await postSigned({
      receiver,
      rawBody,
      signature: independentlySign(rawBody, timestamp, wrongSecret),
    });

    expect(response.status).toBe(401);
  });

  it("rejects missing and malformed authentication headers safely", async () => {
    const receiver = await createReceiver();
    const rawBody = Buffer.from('{"value":"headers"}', "utf8");

    const missingSignature = await postSigned({
      receiver,
      rawBody,
      includeSignature: false,
    });
    const missingTimestamp = await postSigned({
      receiver,
      rawBody,
      includeTimestamp: false,
    });
    const malformedSignature = await postSigned({
      receiver,
      rawBody,
      signature: "not-hexadecimal",
    });
    const uppercaseSignature = await postSigned({
      receiver,
      rawBody,
      signature: independentlySign(rawBody).toUpperCase(),
    });

    expect([
      missingSignature.status,
      missingTimestamp.status,
      malformedSignature.status,
      uppercaseSignature.status,
    ]).toEqual([401, 401, 401, 401]);
    expect(
      receiver.getRequests().every((request) => !request.signatureAccepted),
    ).toBe(true);
  });

  it("does not expose body, signature, or secret through logs", async () => {
    const logEntries: unknown[] = [];
    const receiver = await createReceiver({
      secret: receiverSecret,
      log: (entry) => logEntries.push(entry),
    });
    const rawBody = Buffer.from('{"sensitive":"payload-marker"}', "utf8");

    await postSigned({ receiver, rawBody });

    const serializedLogs = JSON.stringify(logEntries);

    expect(serializedLogs).not.toContain("payload-marker");
    expect(serializedLogs).not.toContain(receiverSecret);
    expect(serializedLogs).not.toContain(independentlySign(rawBody));
  });
});

describe("fake receiver behavior controls", () => {
  it("accepts the maximum body size and rejects the next byte without retaining it", async () => {
    const receiver = await createReceiver();
    const maximumBody = Buffer.alloc(MAX_FAKE_RECEIVER_REQUEST_BODY_BYTES, "a");
    const acceptedResponse = await postSigned({
      receiver,
      rawBody: maximumBody,
    });
    const oversizedResponse = await postSigned({
      receiver,
      rawBody: Buffer.alloc(MAX_FAKE_RECEIVER_REQUEST_BODY_BYTES + 1, "b"),
    });

    expect(acceptedResponse.status).toBe(200);
    expect(oversizedResponse.status).toBe(413);
    const requests = receiver.getRequests();

    expect(requests).toHaveLength(2);
    expect(requests[0]?.rawBody.equals(maximumBody)).toBe(true);
    expect(requests[1]).toMatchObject({
      requestNumber: 2,
      signatureAccepted: false,
      statusCode: 413,
      outcome: "payload_too_large",
    });
    expect(requests[1]?.rawBody).toHaveLength(0);
  });

  it("retains only the most recent bounded request history", async () => {
    const receiver = await createReceiver();
    const totalRequests = MAX_FAKE_RECEIVER_REQUEST_HISTORY + 2;

    for (let index = 1; index <= totalRequests; index += 1) {
      const response = await postSigned({
        receiver,
        rawBody: Buffer.from(String(index), "utf8"),
      });

      expect(response.status).toBe(200);
    }

    const requests = receiver.getRequests();

    expect(requests).toHaveLength(MAX_FAKE_RECEIVER_REQUEST_HISTORY);
    expect(requests[0]?.requestNumber).toBe(3);
    expect(requests.at(-1)?.requestNumber).toBe(totalRequests);
  });

  it("returns a controlled HTTP 500 failure", async () => {
    const receiver = await createReceiver({
      secret: receiverSecret,
      mode: "failure",
    });
    const rawBody = Buffer.from('{"mode":"failure"}', "utf8");

    const response = await postSigned({ receiver, rawBody });

    expect(response.status).toBe(500);
    expect(receiver.getRequests()[0]).toMatchObject({
      signatureAccepted: true,
      mode: "failure",
      statusCode: 500,
      outcome: "failure",
    });
  });

  it("delays timeout-mode responses long enough for a real client abort", async () => {
    const receiver = await createReceiver({
      secret: receiverSecret,
      mode: "timeout",
      delayMs: 250,
    });
    const rawBody = Buffer.from('{"mode":"timeout"}', "utf8");

    await expect(
      postSigned({
        receiver,
        rawBody,
        signal: AbortSignal.timeout(30),
      }),
    ).rejects.toThrow();

    await new Promise((resolve) => setTimeout(resolve, 20));

    expect(receiver.getRequests()[0]).toMatchObject({
      signatureAccepted: true,
      mode: "timeout",
      statusCode: null,
      outcome: "client_aborted",
    });
  });

  it("serves a deterministic failure, failure, success sequence", async () => {
    const receiver = await createReceiver({ secret: receiverSecret });

    receiver.setModeSequence(["failure", "failure", "success"]);

    const statuses: number[] = [];

    for (let index = 0; index < 4; index += 1) {
      const rawBody = Buffer.from(JSON.stringify({ index }), "utf8");
      const response = await postSigned({ receiver, rawBody });

      statuses.push(response.status);
    }

    expect(statuses).toEqual([500, 500, 200, 200]);
    expect(receiver.getRequests().map((request) => request.mode)).toEqual([
      "failure",
      "failure",
      "success",
      "success",
    ]);
  });

  it("does not consume the behavior sequence for rejected signatures", async () => {
    const receiver = await createReceiver({ secret: receiverSecret });

    receiver.setModeSequence(["failure", "success"]);

    const rejectedBody = Buffer.from('{"request":"rejected"}', "utf8");
    const rejectedResponse = await postSigned({
      receiver,
      rawBody: rejectedBody,
      signature: "0".repeat(64),
    });
    const firstAcceptedResponse = await postSigned({
      receiver,
      rawBody: Buffer.from('{"request":"accepted"}', "utf8"),
    });

    expect(rejectedResponse.status).toBe(401);
    expect(firstAcceptedResponse.status).toBe(500);
  });

  it("allows direct mode and delay changes with validation", async () => {
    const receiver = await createReceiver({ secret: receiverSecret });

    receiver.setModeSequence(["failure", "success"]);
    receiver.setMode("timeout");
    receiver.setDelayMs(5);

    const response = await postSigned({
      receiver,
      rawBody: Buffer.from('{"mode":"changed"}', "utf8"),
    });

    expect(response.status).toBe(200);
    expect(receiver.getRequests()[0]).toMatchObject({
      mode: "timeout",
      outcome: "delayed_success",
    });
    expect(() => receiver.setModeSequence([])).toThrow(/must not be empty/);
    expect(() => receiver.setDelayMs(0)).toThrow(/between 1 and/);
  });

  it("closes immediately even with a delayed response in flight", async () => {
    const receiver = await createReceiver({
      secret: receiverSecret,
      mode: "timeout",
      delayMs: 10_000,
    });
    const rawBody = Buffer.from('{"mode":"close"}', "utf8");
    const pendingRequest = postSigned({ receiver, rawBody });

    while (receiver.getRequests().length === 0) {
      await new Promise((resolve) => setTimeout(resolve, 5));
    }

    await receiver.close();

    await expect(pendingRequest).rejects.toThrow();
    expect(receiver.getRequests()[0]?.outcome).toBe("receiver_closed");

    await expect(receiver.close()).resolves.toBeUndefined();
  });
});
