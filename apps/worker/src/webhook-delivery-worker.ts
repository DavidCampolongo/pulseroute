import { randomUUID } from "node:crypto";

import { Prisma, type DatabaseClient } from "@pulseroute/db";
import {
  EVENT_TYPES,
  JOB_NAMES,
  QUEUE_NAMES,
  type WebhookDeliveryJobData,
} from "@pulseroute/shared";
import { type Processor, Worker } from "bullmq";
import type { Logger } from "pino";
import { z } from "zod";

import { readDatabaseNow } from "./database-clock.js";
import { calculateFullJitterDelay } from "./delivery-backoff.js";
import { createJobLogger } from "./logger.js";
import {
  createOutboundWebhookTimestamp,
  sendOutboundWebhook,
  serializeAssignedOutboundWebhook,
  type OutboundWebhookResult,
} from "./outbound-webhook.js";
import { createWorkerRedisOptions } from "./redis.js";

const webhookDeliveryJobSchema = z.strictObject({
  outboxEventId: z.uuid(),
  organizationId: z.uuid(),
  correlationId: z.string().trim().min(1).max(200),
  expectedAttemptNumber: z.number().int().positive(),
  claimStartedAt: z.iso.datetime({ offset: true }),
});

type LockedDeliveryEventRow = {
  outboxEventId: string;
  organizationId: string;
  aggregateId: string;
  eventType: string;
  status: "PENDING" | "PROCESSING" | "DELIVERED" | "EXHAUSTED";
  payload: Prisma.JsonValue;
  attemptCount: number;
  processingStartedAt: Date | null;
  createdAt: Date;
};

type StartedDeliveryAttempt = {
  deliveryAttemptId: string;
  outboxEventId: string;
  organizationId: string;
  serviceRequestId: string;
  correlationId: string;
  attemptNumber: number;
  startedAt: Date;
  body: Uint8Array;
};

type StartDeliveryAttemptResult =
  | {
      kind: "started";
      attempt: StartedDeliveryAttempt;
    }
  | {
      kind: "invalid_outbox_event";
      outboxEventId: string;
      attemptCount: number;
    }
  | {
      kind: "stale_job";
      reason:
        | "terminal_or_pending_event"
        | "claim_version_changed"
        | "attempt_number_changed"
        | "attempt_already_started";
    };

export type WebhookDeliveryProcessorResult =
  | {
      kind: "stale_job";
      reason: StartDeliveryAttemptResult & { kind: "stale_job" } extends infer T
        ? T extends { reason: infer Reason }
          ? Reason
          : never
        : never;
    }
  | {
      kind: "invalid_outbox_event";
      outboxEventId: string;
      attemptCount: number;
    }
  | {
      kind: "delivered";
      outboxEventId: string;
      deliveryAttemptId: string;
      attemptNumber: number;
      httpStatus: number;
      durationMs: number;
    }
  | {
      kind: "retry_scheduled";
      outboxEventId: string;
      deliveryAttemptId: string;
      attemptNumber: number;
      httpStatus: number | null;
      durationMs: number;
      delayMs: number;
      nextAttemptAt: string;
    }
  | {
      kind: "exhausted";
      outboxEventId: string;
      deliveryAttemptId: string;
      attemptNumber: number;
      httpStatus: number | null;
      durationMs: number;
    };

export type WebhookDeliveryProcessorOptions = {
  database: DatabaseClient;
  logger: Logger;
  webhookUrl: string;
  webhookSecret: string;
  timeoutMs: number;
  maxAttempts: number;
  baseDelayMs: number;
  maxDelayMs: number;
  now?: () => Date;
  random?: () => number;
  send?: typeof sendOutboundWebhook;
};

export type WebhookDeliveryWorkerOptions = WebhookDeliveryProcessorOptions & {
  redisUrl: string;
  concurrency?: number;
};

function assertPositiveInteger(value: number, name: string): void {
  if (!Number.isInteger(value) || value < 1) {
    throw new Error(`${name} must be a positive integer`);
  }
}

function validateProcessorOptions(options: WebhookDeliveryProcessorOptions) {
  assertPositiveInteger(options.timeoutMs, "timeoutMs");
  assertPositiveInteger(options.maxAttempts, "maxAttempts");
  assertPositiveInteger(options.baseDelayMs, "baseDelayMs");
  assertPositiveInteger(options.maxDelayMs, "maxDelayMs");

  if (options.maxDelayMs < options.baseDelayMs) {
    throw new Error("maxDelayMs must be greater than or equal to baseDelayMs");
  }
}

export { calculateFullJitterDelay } from "./delivery-backoff.js";

async function startDeliveryAttempt(options: {
  database: DatabaseClient;
  jobData: WebhookDeliveryJobData;
  startedAt: Date;
}): Promise<StartDeliveryAttemptResult> {
  const claimedAt = new Date(options.jobData.claimStartedAt);

  return options.database.$transaction(async (transaction) => {
    const lockedRows = await transaction.$queryRaw<LockedDeliveryEventRow[]>(
      Prisma.sql`
        SELECT
          outbox_event.id AS "outboxEventId",
          outbox_event.organization_id AS "organizationId",
          outbox_event.aggregate_id AS "aggregateId",
          outbox_event.event_type AS "eventType",
          outbox_event.status,
          outbox_event.payload,
          outbox_event.attempt_count AS "attemptCount",
          outbox_event.processing_started_at AS "processingStartedAt",
          outbox_event.created_at AS "createdAt"
        FROM outbox_events AS outbox_event
        WHERE outbox_event.id = ${options.jobData.outboxEventId}::uuid
          AND outbox_event.organization_id = ${options.jobData.organizationId}::uuid
          AND outbox_event.event_type = ${EVENT_TYPES.serviceRequestAssigned}
        FOR UPDATE OF outbox_event
      `,
    );

    const event = lockedRows[0];

    if (!event) {
      throw new Error("Assigned OutboxEvent does not exist for delivery job");
    }

    if (event.status !== "PROCESSING") {
      return {
        kind: "stale_job" as const,
        reason: "terminal_or_pending_event" as const,
      };
    }

    if (event.processingStartedAt?.getTime() !== claimedAt.getTime()) {
      return {
        kind: "stale_job" as const,
        reason: "claim_version_changed" as const,
      };
    }

    if (event.attemptCount + 1 !== options.jobData.expectedAttemptNumber) {
      return {
        kind: "stale_job" as const,
        reason: "attempt_number_changed" as const,
      };
    }

    const existingStartedAttempt = await transaction.webhookDelivery.findFirst({
      where: {
        organizationId: event.organizationId,
        outboxEventId: event.outboxEventId,
        status: "STARTED",
      },
      select: {
        id: true,
      },
    });

    if (existingStartedAttempt) {
      return {
        kind: "stale_job" as const,
        reason: "attempt_already_started" as const,
      };
    }

    let serializedWebhook: ReturnType<typeof serializeAssignedOutboundWebhook>;

    try {
      serializedWebhook = serializeAssignedOutboundWebhook({
        eventId: event.outboxEventId,
        createdAt: event.createdAt,
        payload: event.payload,
      });

      if (
        serializedWebhook.envelope.data.organizationId !==
          options.jobData.organizationId ||
        serializedWebhook.envelope.data.serviceRequestId !==
          event.aggregateId ||
        serializedWebhook.envelope.data.correlationId !==
          options.jobData.correlationId
      ) {
        throw new Error("Assigned OutboxEvent payload identity mismatch");
      }
    } catch {
      await transaction.outboxEvent.update({
        where: {
          id: event.outboxEventId,
        },
        data: {
          status: "EXHAUSTED",
          processingStartedAt: null,
          processedAt: null,
          nextAttemptAt: options.startedAt,
          lastError: {
            code: "INVALID_ASSIGNED_OUTBOX_PAYLOAD",
            message:
              "Assigned OutboxEvent payload is invalid or does not match its durable identity",
            failedAt: options.startedAt.toISOString(),
          },
        },
      });

      return {
        kind: "invalid_outbox_event" as const,
        outboxEventId: event.outboxEventId,
        attemptCount: event.attemptCount,
      };
    }

    const deliveryAttemptId = randomUUID();

    await transaction.webhookDelivery.create({
      data: {
        id: deliveryAttemptId,
        organizationId: event.organizationId,
        outboxEventId: event.outboxEventId,
        attemptNumber: options.jobData.expectedAttemptNumber,
        status: "STARTED",
        startedAt: options.startedAt,
      },
    });

    await transaction.outboxEvent.update({
      where: {
        id: event.outboxEventId,
      },
      data: {
        attemptCount: {
          increment: 1,
        },
        processingStartedAt: options.startedAt,
      },
    });

    return {
      kind: "started" as const,
      attempt: {
        deliveryAttemptId,
        outboxEventId: event.outboxEventId,
        organizationId: event.organizationId,
        serviceRequestId: event.aggregateId,
        correlationId: options.jobData.correlationId,
        attemptNumber: options.jobData.expectedAttemptNumber,
        startedAt: options.startedAt,
        body: serializedWebhook.body,
      },
    };
  });
}

function createFailureEvidence(
  result: Exclude<OutboundWebhookResult, { outcome: "delivered" }>,
  failedAt: Date,
): Prisma.InputJsonObject {
  return {
    ...result.error,
    outcome: result.outcome,
    httpStatus: result.httpStatus,
    durationMs: result.durationMs,
    failedAt: failedAt.toISOString(),
  };
}

async function finishDeliveryAttempt(options: {
  database: DatabaseClient;
  attempt: StartedDeliveryAttempt;
  result: OutboundWebhookResult;
  completedAt: Date;
  maxAttempts: number;
  baseDelayMs: number;
  maxDelayMs: number;
  randomValue: number;
}): Promise<
  Exclude<
    WebhookDeliveryProcessorResult,
    { kind: "stale_job" } | { kind: "invalid_outbox_event" }
  >
> {
  return options.database.$transaction(async (transaction) => {
    await transaction.$queryRaw(
      Prisma.sql`
        SELECT id
        FROM outbox_events
        WHERE id = ${options.attempt.outboxEventId}::uuid
          AND organization_id = ${options.attempt.organizationId}::uuid
        FOR UPDATE
      `,
    );

    const deliveryAttempt = await transaction.webhookDelivery.findFirst({
      where: {
        id: options.attempt.deliveryAttemptId,
        organizationId: options.attempt.organizationId,
        outboxEventId: options.attempt.outboxEventId,
        attemptNumber: options.attempt.attemptNumber,
        status: "STARTED",
      },
      select: {
        id: true,
      },
    });

    if (!deliveryAttempt) {
      throw new Error("WebhookDelivery attempt is no longer STARTED");
    }

    if (options.result.outcome === "delivered") {
      await transaction.webhookDelivery.update({
        where: {
          id: deliveryAttempt.id,
        },
        data: {
          status: "SUCCEEDED",
          completedAt: options.completedAt,
          httpStatus: options.result.httpStatus,
          errorDetails: Prisma.DbNull,
          responseMetadata: {
            durationMs: options.result.durationMs,
          },
        },
      });

      await transaction.outboxEvent.update({
        where: {
          id: options.attempt.outboxEventId,
        },
        data: {
          status: "DELIVERED",
          processingStartedAt: null,
          processedAt: options.completedAt,
          nextAttemptAt: options.completedAt,
          lastError: Prisma.DbNull,
        },
      });

      return {
        kind: "delivered" as const,
        outboxEventId: options.attempt.outboxEventId,
        deliveryAttemptId: options.attempt.deliveryAttemptId,
        attemptNumber: options.attempt.attemptNumber,
        httpStatus: options.result.httpStatus,
        durationMs: options.result.durationMs,
      };
    }

    const failureEvidence = createFailureEvidence(
      options.result,
      options.completedAt,
    );

    await transaction.webhookDelivery.update({
      where: {
        id: deliveryAttempt.id,
      },
      data: {
        status: "FAILED",
        completedAt: options.completedAt,
        httpStatus: options.result.httpStatus,
        errorDetails: failureEvidence,
        responseMetadata: {
          durationMs: options.result.durationMs,
        },
      },
    });

    if (options.attempt.attemptNumber >= options.maxAttempts) {
      await transaction.outboxEvent.update({
        where: {
          id: options.attempt.outboxEventId,
        },
        data: {
          status: "EXHAUSTED",
          processingStartedAt: null,
          processedAt: null,
          nextAttemptAt: options.completedAt,
          lastError: failureEvidence,
        },
      });

      return {
        kind: "exhausted" as const,
        outboxEventId: options.attempt.outboxEventId,
        deliveryAttemptId: options.attempt.deliveryAttemptId,
        attemptNumber: options.attempt.attemptNumber,
        httpStatus: options.result.httpStatus,
        durationMs: options.result.durationMs,
      };
    }

    const { delayMs } = calculateFullJitterDelay({
      retryIndex: options.attempt.attemptNumber - 1,
      baseDelayMs: options.baseDelayMs,
      maxDelayMs: options.maxDelayMs,
      randomValue: options.randomValue,
    });
    const nextAttemptAt = new Date(options.completedAt.getTime() + delayMs);

    await transaction.outboxEvent.update({
      where: {
        id: options.attempt.outboxEventId,
      },
      data: {
        status: "PENDING",
        processingStartedAt: null,
        processedAt: null,
        nextAttemptAt,
        lastError: failureEvidence,
      },
    });

    return {
      kind: "retry_scheduled" as const,
      outboxEventId: options.attempt.outboxEventId,
      deliveryAttemptId: options.attempt.deliveryAttemptId,
      attemptNumber: options.attempt.attemptNumber,
      httpStatus: options.result.httpStatus,
      durationMs: options.result.durationMs,
      delayMs,
      nextAttemptAt: nextAttemptAt.toISOString(),
    };
  });
}

export function createWebhookDeliveryProcessor(
  options: WebhookDeliveryProcessorOptions,
): Processor<WebhookDeliveryJobData, WebhookDeliveryProcessorResult, string> {
  validateProcessorOptions(options);

  const currentTime = options.now
    ? async () => options.now!()
    : async () => readDatabaseNow(options.database);
  const random = options.random ?? Math.random;
  const send = options.send ?? sendOutboundWebhook;

  return async (job) => {
    if (job.name !== JOB_NAMES.deliverWebhook) {
      throw new Error(`Unsupported webhook-delivery job name: ${job.name}`);
    }

    const jobData = webhookDeliveryJobSchema.parse(job.data);
    const jobLogger = createJobLogger(options.logger, {
      queue: QUEUE_NAMES.webhookDelivery,
      jobName: job.name,
      jobId: job.id ?? "unknown",
      attemptsMade: job.attemptsMade,
      organizationId: jobData.organizationId,
      correlationId: jobData.correlationId,
    });
    const startedAt = await currentTime();
    const startResult = await startDeliveryAttempt({
      database: options.database,
      jobData,
      startedAt,
    });

    if (startResult.kind === "stale_job") {
      jobLogger.info(
        {
          outcome: startResult.kind,
          reason: startResult.reason,
          outboxEventId: jobData.outboxEventId,
          expectedAttemptNumber: jobData.expectedAttemptNumber,
        },
        "Webhook delivery job was already superseded",
      );

      return startResult;
    }

    if (startResult.kind === "invalid_outbox_event") {
      jobLogger.error(
        {
          outcome: startResult.kind,
          outboxEventId: startResult.outboxEventId,
          attemptCount: startResult.attemptCount,
        },
        "Invalid assigned OutboxEvent was terminally exhausted",
      );

      return startResult;
    }

    const attempt = startResult.attempt;
    const timestamp = createOutboundWebhookTimestamp(startedAt);
    const deliveryResult = await send({
      url: options.webhookUrl,
      secret: options.webhookSecret,
      timeoutMs: options.timeoutMs,
      timestamp,
      eventId: attempt.outboxEventId,
      body: attempt.body,
    });
    const completedAt = await currentTime();
    const result = await finishDeliveryAttempt({
      database: options.database,
      attempt,
      result: deliveryResult,
      completedAt,
      maxAttempts: options.maxAttempts,
      baseDelayMs: options.baseDelayMs,
      maxDelayMs: options.maxDelayMs,
      randomValue: random(),
    });

    const logFields = {
      outcome: result.kind,
      outboxEventId: result.outboxEventId,
      deliveryAttemptId: result.deliveryAttemptId,
      serviceRequestId: attempt.serviceRequestId,
      attemptNumber: result.attemptNumber,
      httpStatus: result.httpStatus,
      durationMs: result.durationMs,
      ...(result.kind === "retry_scheduled"
        ? {
            delayMs: result.delayMs,
            nextAttemptAt: result.nextAttemptAt,
          }
        : {}),
    };

    if (result.kind === "delivered") {
      jobLogger.info(logFields, "Webhook delivery succeeded");
    } else if (result.kind === "retry_scheduled") {
      jobLogger.warn(logFields, "Webhook delivery retry scheduled");
    } else {
      jobLogger.error(logFields, "Webhook delivery attempts exhausted");
    }

    return result;
  };
}

export function createWebhookDeliveryWorker(
  options: WebhookDeliveryWorkerOptions,
): Worker<WebhookDeliveryJobData, WebhookDeliveryProcessorResult, string> {
  const concurrency = options.concurrency ?? 5;

  assertPositiveInteger(concurrency, "Webhook delivery worker concurrency");

  return new Worker<
    WebhookDeliveryJobData,
    WebhookDeliveryProcessorResult,
    string
  >(QUEUE_NAMES.webhookDelivery, createWebhookDeliveryProcessor(options), {
    connection: createWorkerRedisOptions(options.redisUrl),
    concurrency,
  });
}
