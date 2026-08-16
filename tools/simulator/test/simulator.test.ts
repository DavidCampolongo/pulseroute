import { createHmac } from "node:crypto";
import { createServer, type IncomingMessage, type Server } from "node:http";
import type { AddressInfo } from "node:net";

import { afterEach, describe, expect, it } from "vitest";

import {
  SIMULATOR_FIXTURE,
  runSimulator,
  type SimulatorFetch,
} from "../src/simulator.js";

const secret = "simulator-test-secret-at-least-32-characters";
const runId = "11111111-2222-4333-8444-555555555555";

type ReceivedRequest = {
  path: string;
  rawBody: string;
  timestamp: string | undefined;
  signature: string | undefined;
  signatureValid: boolean;
  payload: Record<string, unknown>;
};

type Oracle = {
  url: string;
  received: ReceivedRequest[];
  close: () => Promise<void>;
};

const openServers = new Set<Server>();

function readSingleHeader(
  request: IncomingMessage,
  name: string,
): string | undefined {
  const value = request.headers[name];

  return typeof value === "string" ? value : undefined;
}

async function readRawBody(request: IncomingMessage): Promise<string> {
  const chunks: Buffer[] = [];

  for await (const chunk of request) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }

  return Buffer.concat(chunks).toString("utf8");
}

async function startOracle(): Promise<Oracle> {
  const received: ReceivedRequest[] = [];
  const serviceRequestIds = new Map<string, string>();

  const server = createServer((request, response) => {
    void (async () => {
      const rawBody = await readRawBody(request);
      const timestamp = readSingleHeader(request, "x-pulseroute-timestamp");
      const signature = readSingleHeader(request, "x-pulseroute-signature");
      const expectedSignature =
        timestamp === undefined
          ? undefined
          : createHmac("sha256", secret)
              .update(Buffer.from(`${timestamp}.${rawBody}`, "utf8"))
              .digest("hex");
      const payload = JSON.parse(rawBody) as Record<string, unknown>;

      received.push({
        path: request.url ?? "",
        rawBody,
        timestamp,
        signature,
        signatureValid:
          expectedSignature !== undefined && signature === expectedSignature,
        payload,
      });

      if (
        request.method !== "POST" ||
        request.url !== "/webhooks/service-requests" ||
        request.headers["content-type"] !== "application/json" ||
        expectedSignature === undefined ||
        signature !== expectedSignature
      ) {
        response.writeHead(401, {
          "content-type": "application/json",
        });
        response.end(JSON.stringify({ code: "AUTHENTICATION_FAILED" }));
        return;
      }

      const data = payload.data as Record<string, unknown>;
      const externalId = data.externalId as string;
      const existingId = serviceRequestIds.get(externalId);
      const serviceRequestId =
        existingId ??
        `90000000-0000-4000-8000-${String(serviceRequestIds.size + 1).padStart(12, "0")}`;
      const status = existingId === undefined ? "accepted" : "duplicate";

      serviceRequestIds.set(externalId, serviceRequestId);

      response.writeHead(status === "accepted" ? 202 : 200, {
        "content-type": "application/json",
      });
      response.end(
        JSON.stringify({
          requestId: `oracle-request-${received.length}`,
          status,
          serviceRequestId,
        }),
      );
    })().catch(() => {
      if (!response.headersSent) {
        response.writeHead(500);
      }

      response.end();
    });
  });

  openServers.add(server);

  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      server.removeListener("error", reject);
      resolve();
    });
  });

  const address = server.address() as AddressInfo;

  return {
    url: `http://127.0.0.1:${address.port}/webhooks/service-requests`,
    received,
    close: async () => {
      openServers.delete(server);

      await new Promise<void>((resolve, reject) => {
        server.close((error) => {
          if (error) {
            reject(error);
          } else {
            resolve();
          }
        });
      });
    },
  };
}

afterEach(async () => {
  await Promise.all(
    [...openServers].map(
      (server) =>
        new Promise<void>((resolve) => {
          server.close(() => resolve());
        }),
    ),
  );

  openServers.clear();
});

describe("runSimulator", () => {
  it("sends independently signed fixture traffic and exact-body duplicate replays", async () => {
    const oracle = await startOracle();
    const observedResults: unknown[] = [];

    const summary = await runSimulator({
      endpoint: oracle.url,
      count: 3,
      duplicates: 2,
      webhookSecret: secret,
      timeoutMs: 1_000,
      now: () => new Date("2026-08-15T12:00:00.000Z"),
      createRunId: () => runId,
      onResult: (result) => observedResults.push(result),
    });

    await oracle.close();

    expect(summary).toMatchObject({
      runId,
      endpoint: oracle.url,
      requestedCount: 3,
      requestedDuplicates: 2,
      attempted: 5,
      accepted: 3,
      duplicate: 2,
      errors: 0,
    });
    expect(summary.results).toHaveLength(5);
    expect(observedResults).toHaveLength(5);
    expect(oracle.received).toHaveLength(5);

    expect(oracle.received[3]?.rawBody).toBe(oracle.received[0]?.rawBody);
    expect(oracle.received[4]?.rawBody).toBe(oracle.received[1]?.rawBody);

    for (const [index, request] of oracle.received.entries()) {
      expect(request.path).toBe("/webhooks/service-requests");
      expect(request.timestamp).toBe("1786795200");
      expect(request.signature).toMatch(/^[0-9a-f]{64}$/);
      expect(request.signatureValid).toBe(true);
      expect(request.payload).toMatchObject({
        organizationId: SIMULATOR_FIXTURE.organizationId,
        type: "service_request.created",
        data: {
          requiredSkillId: SIMULATOR_FIXTURE.requiredSkillId,
          priority: "HIGH",
          region: "NORTH",
        },
      });

      const sourceSequence = (index % 3) + 1;

      if (index < 3) {
        expect(request.payload.eventId).toBe(
          `simulator-event-${runId}-${sourceSequence}`,
        );
        expect(
          (request.payload.data as Record<string, unknown>).externalId,
        ).toBe(`simulator-request-${runId}-${sourceSequence}`);
      }
    }

    expect(summary.results[3]?.serviceRequestId).toBe(
      summary.results[0]?.serviceRequestId,
    );
    expect(summary.results[4]?.serviceRequestId).toBe(
      summary.results[1]?.serviceRequestId,
    );
    expect(JSON.stringify(summary)).not.toContain(secret);
    expect(JSON.stringify(summary)).not.toContain("x-pulseroute-signature");
  });

  it("continues after a server error and retains a failing summary", async () => {
    let callCount = 0;

    const fetch: SimulatorFetch = async () => {
      callCount += 1;

      if (callCount === 2) {
        return new Response(
          JSON.stringify({
            requestId: "server-error",
            code: "INTERNAL_ERROR",
            message: "An unexpected error occurred",
          }),
          {
            status: 500,
            headers: {
              "content-type": "application/json",
            },
          },
        );
      }

      const duplicate = callCount === 3;

      return new Response(
        JSON.stringify({
          requestId: `request-${callCount}`,
          status: duplicate ? "duplicate" : "accepted",
          serviceRequestId: "90000000-0000-4000-8000-000000000001",
        }),
        {
          status: duplicate ? 200 : 202,
          headers: {
            "content-type": "application/json",
          },
        },
      );
    };

    const summary = await runSimulator({
      endpoint: "https://pulseroute.example/webhooks/service-requests",
      count: 2,
      duplicates: 1,
      webhookSecret: secret,
      timeoutMs: 1_000,
      fetch,
      createRunId: () => runId,
    });

    expect(callCount).toBe(3);
    expect(summary).toMatchObject({
      attempted: 3,
      accepted: 1,
      duplicate: 1,
      errors: 1,
    });
    expect(summary.results[1]).toMatchObject({
      statusCode: 500,
      outcome: "error",
      errorCode: "UNEXPECTED_RESPONSE",
    });
  });

  it("bounds a stalled request with a deterministic timeout result", async () => {
    const fetch: SimulatorFetch = async (_input, init) =>
      new Promise<Response>((_resolve, reject) => {
        init.signal?.addEventListener(
          "abort",
          () => reject(new Error("aborted")),
          {
            once: true,
          },
        );
      });

    const summary = await runSimulator({
      endpoint: "https://pulseroute.example/webhooks/service-requests",
      count: 1,
      duplicates: 0,
      webhookSecret: secret,
      timeoutMs: 5,
      fetch,
      createRunId: () => runId,
    });

    expect(summary).toMatchObject({
      attempted: 1,
      accepted: 0,
      duplicate: 0,
      errors: 1,
    });
    expect(summary.results[0]).toMatchObject({
      statusCode: null,
      outcome: "error",
      errorCode: "REQUEST_TIMEOUT",
    });
  });
});
