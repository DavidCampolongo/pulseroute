import { randomUUID } from "node:crypto";

import type { SimulatorConfig } from "./config.js";
import { createSignedWebhookHeaders } from "./signing.js";

const MAX_RESPONSE_BODY_BYTES = 64 * 1024;

const uuidPattern =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

export const SIMULATOR_FIXTURE = {
  organizationId: "00000001-0000-4000-8000-000000000001",
  requiredSkillId: "00000003-0000-4000-8000-000000000001",
  priority: "HIGH",
  region: "NORTH",
} as const;

export type SimulatorRequestKind = "unique" | "duplicate";

export type SimulatorResultOutcome = "accepted" | "duplicate" | "error";

export type SimulatorErrorCode =
  | "NETWORK_ERROR"
  | "REQUEST_TIMEOUT"
  | "UNEXPECTED_RESPONSE"
  | "DUPLICATE_ID_MISMATCH";

export type SimulatorRequestResult = {
  requestNumber: number;
  kind: SimulatorRequestKind;
  sourceSequence: number;
  eventId: string;
  externalId: string;
  statusCode: number | null;
  outcome: SimulatorResultOutcome;
  requestId: string | null;
  serviceRequestId: string | null;
  errorCode: SimulatorErrorCode | null;
};

export type SimulatorSummary = {
  runId: string;
  endpoint: string;
  requestedCount: number;
  requestedDuplicates: number;
  attempted: number;
  accepted: number;
  duplicate: number;
  errors: number;
  results: readonly SimulatorRequestResult[];
};

export type SimulatorFetch = (
  input: string,
  init: RequestInit,
) => Promise<Response>;

export type RunSimulatorOptions = Pick<
  SimulatorConfig,
  "endpoint" | "count" | "duplicates" | "webhookSecret" | "timeoutMs"
> & {
  fetch?: SimulatorFetch;
  now?: () => Date;
  createRunId?: () => string;
  onResult?: (result: SimulatorRequestResult) => void;
};

type PlannedRequest = {
  sourceSequence: number;
  eventId: string;
  externalId: string;
  rawBody: string;
};

type ContractResponse = {
  status: "accepted" | "duplicate";
  requestId: string;
  serviceRequestId: string;
};

function createRequestPlan(count: number, runId: string): PlannedRequest[] {
  return Array.from({ length: count }, (_, index) => {
    const sourceSequence = index + 1;
    const externalId = `simulator-request-${runId}-${sourceSequence}`;
    const eventId = `simulator-event-${runId}-${sourceSequence}`;

    return {
      sourceSequence,
      eventId,
      externalId,
      rawBody: JSON.stringify({
        organizationId: SIMULATOR_FIXTURE.organizationId,
        eventId,
        type: "service_request.created",
        data: {
          externalId,
          requiredSkillId: SIMULATOR_FIXTURE.requiredSkillId,
          priority: SIMULATOR_FIXTURE.priority,
          region: SIMULATOR_FIXTURE.region,
        },
      }),
    };
  });
}

function readContractResponse(value: unknown): ContractResponse | null {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return null;
  }

  const record = value as Record<string, unknown>;
  const status = record.status;
  const requestId = record.requestId;
  const serviceRequestId = record.serviceRequestId;

  if (status !== "accepted" && status !== "duplicate") {
    return null;
  }

  if (
    typeof requestId !== "string" ||
    requestId.length === 0 ||
    requestId.length > 500
  ) {
    return null;
  }

  if (
    typeof serviceRequestId !== "string" ||
    !uuidPattern.test(serviceRequestId)
  ) {
    return null;
  }

  return {
    status,
    requestId,
    serviceRequestId,
  };
}

async function readBoundedResponseBody(
  response: Response,
): Promise<string | null> {
  const declaredLength = response.headers.get("content-length");

  if (declaredLength !== null && /^\d+$/.test(declaredLength)) {
    const parsedLength = Number(declaredLength);

    if (
      Number.isSafeInteger(parsedLength) &&
      parsedLength > MAX_RESPONSE_BODY_BYTES
    ) {
      await response.body?.cancel().catch(() => undefined);
      return null;
    }
  }

  if (response.body === null) {
    return "";
  }

  const reader = response.body.getReader();
  const chunks: Buffer[] = [];
  let totalBytes = 0;

  try {
    while (true) {
      const chunk = await reader.read();

      if (chunk.done) {
        break;
      }

      totalBytes += chunk.value.byteLength;

      if (totalBytes > MAX_RESPONSE_BODY_BYTES) {
        await reader.cancel().catch(() => undefined);
        return null;
      }

      chunks.push(Buffer.from(chunk.value));
    }
  } finally {
    reader.releaseLock();
  }

  return Buffer.concat(chunks, totalBytes).toString("utf8");
}

async function parseResponseBody(response: Response): Promise<unknown> {
  const responseText = await readBoundedResponseBody(response);

  if (responseText === null) {
    return null;
  }

  try {
    return JSON.parse(responseText) as unknown;
  } catch {
    return null;
  }
}

function createErrorResult(options: {
  requestNumber: number;
  kind: SimulatorRequestKind;
  request: PlannedRequest;
  statusCode: number | null;
  errorCode: SimulatorErrorCode;
  contractResponse?: ContractResponse;
}): SimulatorRequestResult {
  return {
    requestNumber: options.requestNumber,
    kind: options.kind,
    sourceSequence: options.request.sourceSequence,
    eventId: options.request.eventId,
    externalId: options.request.externalId,
    statusCode: options.statusCode,
    outcome: "error",
    requestId: options.contractResponse?.requestId ?? null,
    serviceRequestId: options.contractResponse?.serviceRequestId ?? null,
    errorCode: options.errorCode,
  };
}

async function sendPlannedRequest(options: {
  endpoint: string;
  secret: string;
  timeoutMs: number;
  fetch: SimulatorFetch;
  now: () => Date;
  requestNumber: number;
  kind: SimulatorRequestKind;
  request: PlannedRequest;
  expectedServiceRequestId?: string;
}): Promise<SimulatorRequestResult> {
  const timestampMilliseconds = options.now().getTime();

  if (!Number.isFinite(timestampMilliseconds) || timestampMilliseconds <= 0) {
    throw new Error("Simulator clock must return a valid positive Date");
  }

  const timestamp = String(Math.floor(timestampMilliseconds / 1_000));
  const abortController = new AbortController();
  const timeout = setTimeout(() => {
    abortController.abort();
  }, options.timeoutMs);

  try {
    const response = await options.fetch(options.endpoint, {
      method: "POST",
      headers: createSignedWebhookHeaders({
        secret: options.secret,
        timestamp,
        rawBody: options.request.rawBody,
      }),
      body: options.request.rawBody,
      signal: abortController.signal,
    });

    const contractResponse = readContractResponse(
      await parseResponseBody(response),
    );

    const expectedStatusCode = options.kind === "unique" ? 202 : 200;
    const expectedStatus = options.kind === "unique" ? "accepted" : "duplicate";

    if (
      response.status !== expectedStatusCode ||
      contractResponse?.status !== expectedStatus
    ) {
      return createErrorResult({
        requestNumber: options.requestNumber,
        kind: options.kind,
        request: options.request,
        statusCode: response.status,
        errorCode: "UNEXPECTED_RESPONSE",
        ...(contractResponse ? { contractResponse } : {}),
      });
    }

    if (
      options.expectedServiceRequestId !== undefined &&
      contractResponse.serviceRequestId !== options.expectedServiceRequestId
    ) {
      return createErrorResult({
        requestNumber: options.requestNumber,
        kind: options.kind,
        request: options.request,
        statusCode: response.status,
        errorCode: "DUPLICATE_ID_MISMATCH",
        contractResponse,
      });
    }

    return {
      requestNumber: options.requestNumber,
      kind: options.kind,
      sourceSequence: options.request.sourceSequence,
      eventId: options.request.eventId,
      externalId: options.request.externalId,
      statusCode: response.status,
      outcome: contractResponse.status,
      requestId: contractResponse.requestId,
      serviceRequestId: contractResponse.serviceRequestId,
      errorCode: null,
    };
  } catch {
    return createErrorResult({
      requestNumber: options.requestNumber,
      kind: options.kind,
      request: options.request,
      statusCode: null,
      errorCode: abortController.signal.aborted
        ? "REQUEST_TIMEOUT"
        : "NETWORK_ERROR",
    });
  } finally {
    clearTimeout(timeout);
  }
}

export async function runSimulator(
  options: RunSimulatorOptions,
): Promise<SimulatorSummary> {
  const fetchImplementation = options.fetch ?? globalThis.fetch;
  const now = options.now ?? (() => new Date());
  const runId = (options.createRunId ?? randomUUID)();
  const requests = createRequestPlan(options.count, runId);
  const results: SimulatorRequestResult[] = [];
  const acceptedServiceRequestIds = new Map<number, string>();

  for (const request of requests) {
    const result = await sendPlannedRequest({
      endpoint: options.endpoint,
      secret: options.webhookSecret,
      timeoutMs: options.timeoutMs,
      fetch: fetchImplementation,
      now,
      requestNumber: results.length + 1,
      kind: "unique",
      request,
    });

    results.push(result);

    if (result.outcome === "accepted" && result.serviceRequestId !== null) {
      acceptedServiceRequestIds.set(
        request.sourceSequence,
        result.serviceRequestId,
      );
    }

    options.onResult?.(result);
  }

  for (let index = 0; index < options.duplicates; index += 1) {
    const request = requests[index % requests.length];

    if (!request) {
      throw new Error("Simulator request-plan invariant failed");
    }

    const expectedServiceRequestId = acceptedServiceRequestIds.get(
      request.sourceSequence,
    );

    const result = await sendPlannedRequest({
      endpoint: options.endpoint,
      secret: options.webhookSecret,
      timeoutMs: options.timeoutMs,
      fetch: fetchImplementation,
      now,
      requestNumber: results.length + 1,
      kind: "duplicate",
      request,
      ...(expectedServiceRequestId === undefined
        ? {}
        : {
            expectedServiceRequestId,
          }),
    });

    results.push(result);
    options.onResult?.(result);
  }

  return {
    runId,
    endpoint: options.endpoint,
    requestedCount: options.count,
    requestedDuplicates: options.duplicates,
    attempted: results.length,
    accepted: results.filter((result) => result.outcome === "accepted").length,
    duplicate: results.filter((result) => result.outcome === "duplicate")
      .length,
    errors: results.filter((result) => result.outcome === "error").length,
    results,
  };
}
