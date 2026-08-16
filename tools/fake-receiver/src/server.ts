import { Buffer } from "node:buffer";
import { createHmac, timingSafeEqual } from "node:crypto";
import {
  createServer,
  type IncomingMessage,
  type ServerResponse,
} from "node:http";

import {
  DEFAULT_RECEIVER_DELAY_MS,
  MAX_RECEIVER_DELAY_MS,
  type FakeReceiverMode,
} from "./config.js";

export const FAKE_RECEIVER_HOST = "127.0.0.1";
export const FAKE_RECEIVER_WEBHOOK_PATH = "/webhooks";
export const OUTBOUND_WEBHOOK_TIMESTAMP_HEADER = "x-pulseroute-timestamp";
export const OUTBOUND_WEBHOOK_SIGNATURE_HEADER = "x-pulseroute-signature";
export const MAX_FAKE_RECEIVER_REQUEST_BODY_BYTES = 64 * 1_024;
export const MAX_FAKE_RECEIVER_REQUEST_HISTORY = 100;

const hexadecimalSha256Pattern = /^[0-9a-f]{64}$/;
const timestampPattern = /^(0|[1-9]\d*)$/;

export type FakeReceiverRequestOutcome =
  | "pending"
  | "signature_rejected"
  | "payload_too_large"
  | "success"
  | "failure"
  | "delayed_success"
  | "client_aborted"
  | "receiver_closed";

export type FakeReceiverRequest = {
  requestNumber: number;
  receivedAt: string;
  method: string;
  path: string;
  timestamp: string | null;
  signatureAccepted: boolean;
  mode: FakeReceiverMode | null;
  statusCode: number | null;
  outcome: FakeReceiverRequestOutcome;
  rawBody: Buffer;
};

export type FakeReceiverLogEntry = Omit<FakeReceiverRequest, "rawBody">;

export type StartFakeReceiverOptions = {
  secret: string;
  host?: string;
  port?: number;
  mode?: FakeReceiverMode;
  delayMs?: number;
  log?: (entry: FakeReceiverLogEntry) => void;
};

export type RunningFakeReceiver = {
  readonly host: string;
  readonly port: number;
  readonly url: string;
  setMode(mode: FakeReceiverMode): void;
  setModeSequence(modes: readonly FakeReceiverMode[]): void;
  setDelayMs(delayMs: number): void;
  getRequests(): FakeReceiverRequest[];
  close(): Promise<void>;
};

type MutableFakeReceiverRequest = FakeReceiverRequest;

type PendingResponse = {
  timer: NodeJS.Timeout;
  request: MutableFakeReceiverRequest;
};

function assertSecret(secret: string): void {
  if (secret.trim().length === 0) {
    throw new Error("Fake receiver secret must not be blank");
  }

  if (secret !== secret.trim()) {
    throw new Error(
      "Fake receiver secret must not have leading or trailing whitespace",
    );
  }

  if (secret.length < 32) {
    throw new Error("Fake receiver secret must be at least 32 characters");
  }
}

function assertPort(port: number): void {
  if (!Number.isInteger(port) || port < 0 || port > 65_535) {
    throw new Error("Fake receiver port must be between 0 and 65535");
  }
}

function assertDelayMs(delayMs: number): void {
  if (
    !Number.isInteger(delayMs) ||
    delayMs < 1 ||
    delayMs > MAX_RECEIVER_DELAY_MS
  ) {
    throw new Error(
      `Fake receiver delay must be between 1 and ${MAX_RECEIVER_DELAY_MS} milliseconds`,
    );
  }
}

function cloneRequest(request: FakeReceiverRequest): FakeReceiverRequest {
  return {
    ...request,
    rawBody: Buffer.from(request.rawBody),
  };
}

function readSingleHeader(
  request: IncomingMessage,
  headerName: string,
): string | undefined {
  const headerValue = request.headers[headerName];

  return typeof headerValue === "string" ? headerValue : undefined;
}

type RawBodyReadResult =
  | {
      kind: "complete";
      rawBody: Buffer;
    }
  | {
      kind: "payload_too_large";
    };

function readRawBody(request: IncomingMessage): Promise<RawBodyReadResult> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    let totalBytes = 0;
    let payloadTooLarge = false;

    request.on("data", (chunk: Buffer) => {
      if (payloadTooLarge) {
        return;
      }

      const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);

      totalBytes += buffer.byteLength;

      if (totalBytes > MAX_FAKE_RECEIVER_REQUEST_BODY_BYTES) {
        payloadTooLarge = true;
        chunks.length = 0;
        resolve({ kind: "payload_too_large" });

        return;
      }

      chunks.push(buffer);
    });

    request.once("end", () => {
      if (!payloadTooLarge) {
        resolve({
          kind: "complete",
          rawBody: Buffer.concat(chunks, totalBytes),
        });
      }
    });

    request.once("error", (error) => {
      if (!payloadTooLarge) {
        reject(error);
      }
    });
  });
}

function verifySignature(options: {
  secret: string;
  timestamp: string | undefined;
  signature: string | undefined;
  rawBody: Buffer;
}): boolean {
  if (
    options.timestamp === undefined ||
    !timestampPattern.test(options.timestamp) ||
    options.signature === undefined ||
    !hexadecimalSha256Pattern.test(options.signature)
  ) {
    return false;
  }

  const expectedSignature = createHmac("sha256", options.secret)
    .update(options.timestamp, "utf8")
    .update(".", "utf8")
    .update(options.rawBody)
    .digest();

  const receivedSignature = Buffer.from(options.signature, "hex");

  return (
    expectedSignature.length === receivedSignature.length &&
    timingSafeEqual(expectedSignature, receivedSignature)
  );
}

function sendJson(response: ServerResponse, statusCode: number): void {
  const responseBody = Buffer.from(
    JSON.stringify({
      ok: statusCode >= 200 && statusCode < 300,
    }),
    "utf8",
  );

  response.writeHead(statusCode, {
    "content-type": "application/json; charset=utf-8",
    "content-length": String(responseBody.byteLength),
  });
  response.end(responseBody);
}

function formatHostForUrl(host: string): string {
  return host.includes(":") ? `[${host}]` : host;
}

export async function startFakeReceiver(
  options: StartFakeReceiverOptions,
): Promise<RunningFakeReceiver> {
  assertSecret(options.secret);

  const host = options.host ?? FAKE_RECEIVER_HOST;
  const requestedPort = options.port ?? 0;
  let currentMode = options.mode ?? "success";
  let currentDelayMs = options.delayMs ?? DEFAULT_RECEIVER_DELAY_MS;

  if (host.trim().length === 0) {
    throw new Error("Fake receiver host must not be empty");
  }

  assertPort(requestedPort);
  assertDelayMs(currentDelayMs);

  const requests: MutableFakeReceiverRequest[] = [];
  const pendingResponses = new Map<ServerResponse, PendingResponse>();
  let modeSequence: FakeReceiverMode[] = [];
  let modeSequenceIndex = 0;
  let requestNumber = 0;
  let closePromise: Promise<void> | undefined;

  function recordLog(request: MutableFakeReceiverRequest): void {
    if (!options.log) {
      return;
    }

    options.log({
      requestNumber: request.requestNumber,
      receivedAt: request.receivedAt,
      method: request.method,
      path: request.path,
      timestamp: request.timestamp,
      signatureAccepted: request.signatureAccepted,
      mode: request.mode,
      statusCode: request.statusCode,
      outcome: request.outcome,
    });
  }

  function retainRequest(request: MutableFakeReceiverRequest): void {
    requests.push(request);

    if (requests.length > MAX_FAKE_RECEIVER_REQUEST_HISTORY) {
      requests.splice(0, requests.length - MAX_FAKE_RECEIVER_REQUEST_HISTORY);
    }
  }

  function nextMode(): FakeReceiverMode {
    if (modeSequence.length === 0) {
      return currentMode;
    }

    const selectedMode =
      modeSequence[Math.min(modeSequenceIndex, modeSequence.length - 1)]!;

    modeSequenceIndex += 1;

    return selectedMode;
  }

  async function handleWebhookRequest(
    request: IncomingMessage,
    response: ServerResponse,
  ): Promise<void> {
    const rawBodyResult = await readRawBody(request);
    const timestamp = readSingleHeader(
      request,
      OUTBOUND_WEBHOOK_TIMESTAMP_HEADER,
    );
    const signature = readSingleHeader(
      request,
      OUTBOUND_WEBHOOK_SIGNATURE_HEADER,
    );

    if (rawBodyResult.kind === "payload_too_large") {
      requestNumber += 1;

      const recordedRequest: MutableFakeReceiverRequest = {
        requestNumber,
        receivedAt: new Date().toISOString(),
        method: request.method ?? "UNKNOWN",
        path: new URL(request.url ?? "/", "http://localhost").pathname,
        timestamp: timestamp ?? null,
        signatureAccepted: false,
        mode: null,
        statusCode: 413,
        outcome: "payload_too_large",
        rawBody: Buffer.alloc(0),
      };

      retainRequest(recordedRequest);
      sendJson(response, 413);
      recordLog(recordedRequest);

      return;
    }

    const rawBody = rawBodyResult.rawBody;
    const signatureAccepted = verifySignature({
      secret: options.secret,
      timestamp,
      signature,
      rawBody,
    });

    requestNumber += 1;

    const recordedRequest: MutableFakeReceiverRequest = {
      requestNumber,
      receivedAt: new Date().toISOString(),
      method: request.method ?? "UNKNOWN",
      path: new URL(request.url ?? "/", "http://localhost").pathname,
      timestamp: timestamp ?? null,
      signatureAccepted,
      mode: null,
      statusCode: null,
      outcome: "pending",
      rawBody: Buffer.from(rawBody),
    };

    retainRequest(recordedRequest);

    if (!signatureAccepted) {
      recordedRequest.statusCode = 401;
      recordedRequest.outcome = "signature_rejected";

      sendJson(response, recordedRequest.statusCode);
      recordLog(recordedRequest);

      return;
    }

    const mode = nextMode();

    recordedRequest.mode = mode;

    if (mode === "success") {
      recordedRequest.statusCode = 200;
      recordedRequest.outcome = "success";

      sendJson(response, recordedRequest.statusCode);
      recordLog(recordedRequest);

      return;
    }

    if (mode === "failure") {
      recordedRequest.statusCode = 500;
      recordedRequest.outcome = "failure";

      sendJson(response, recordedRequest.statusCode);
      recordLog(recordedRequest);

      return;
    }

    const timer = setTimeout(() => {
      pendingResponses.delete(response);

      if (response.destroyed) {
        recordedRequest.outcome = "client_aborted";
        recordLog(recordedRequest);

        return;
      }

      recordedRequest.statusCode = 200;
      recordedRequest.outcome = "delayed_success";

      sendJson(response, recordedRequest.statusCode);
      recordLog(recordedRequest);
    }, currentDelayMs);

    pendingResponses.set(response, {
      timer,
      request: recordedRequest,
    });

    response.once("close", () => {
      if (!pendingResponses.delete(response)) {
        return;
      }

      clearTimeout(timer);
      recordedRequest.outcome = "client_aborted";
      recordLog(recordedRequest);
    });
  }

  const server = createServer((request, response) => {
    const requestPath = new URL(request.url ?? "/", "http://localhost")
      .pathname;

    if (requestPath !== FAKE_RECEIVER_WEBHOOK_PATH) {
      request.resume();
      sendJson(response, 404);

      return;
    }

    if (request.method !== "POST") {
      request.resume();
      sendJson(response, 405);

      return;
    }

    void handleWebhookRequest(request, response).catch(() => {
      if (!response.headersSent && !response.destroyed) {
        sendJson(response, 500);
      } else if (!response.destroyed) {
        response.destroy();
      }
    });
  });

  await new Promise<void>((resolve, reject) => {
    const handleListenError = (error: Error) => {
      reject(error);
    };

    server.once("error", handleListenError);
    server.listen(requestedPort, host, () => {
      server.off("error", handleListenError);
      resolve();
    });
  });

  const address = server.address();

  if (address === null || typeof address === "string") {
    server.close();

    throw new Error("Fake receiver did not bind to a TCP port");
  }

  const port = address.port;
  const url = `http://${formatHostForUrl(host)}:${port}${FAKE_RECEIVER_WEBHOOK_PATH}`;

  return {
    host,
    port,
    url,

    setMode(mode) {
      currentMode = mode;
      modeSequence = [];
      modeSequenceIndex = 0;
    },

    setModeSequence(modes) {
      if (modes.length === 0) {
        throw new Error("Fake receiver mode sequence must not be empty");
      }

      modeSequence = [...modes];
      modeSequenceIndex = 0;
      currentMode = modeSequence[modeSequence.length - 1]!;
    },

    setDelayMs(delayMs) {
      assertDelayMs(delayMs);
      currentDelayMs = delayMs;
    },

    getRequests() {
      return requests.map(cloneRequest);
    },

    close() {
      if (!closePromise) {
        closePromise = (async () => {
          for (const [response, pendingResponse] of pendingResponses) {
            clearTimeout(pendingResponse.timer);
            pendingResponse.request.outcome = "receiver_closed";
            recordLog(pendingResponse.request);
            response.destroy();
          }

          pendingResponses.clear();

          await new Promise<void>((resolve, reject) => {
            server.close((error) => {
              if (error) {
                reject(error);

                return;
              }

              resolve();
            });

            server.closeAllConnections();
          });
        })();
      }

      return closePromise;
    },
  };
}
