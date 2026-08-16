import { Prisma, type DatabaseClient } from "@pulseroute/db";

import {
  calculateFullJitterDelay,
  DEFAULT_WEBHOOK_DELIVERY_BASE_DELAY_MS,
  DEFAULT_WEBHOOK_DELIVERY_MAX_DELAY_MS,
} from "./delivery-backoff.js";

const ASSIGNED_EVENT_TYPE = "service_request.assigned";

type LockedOutboxEventStatus = "PENDING" | "PROCESSING";

type LockedOutboxEventRow = {
  outboxEventId: string;
  organizationId: string;
  aggregateId: string;
  payload: Prisma.JsonValue;
  status: LockedOutboxEventStatus;
  attemptCount: number;
  processingStartedAt: Date | null;
};

type LockedExhaustedOutboxEventRow = {
  outboxEventId: string;
  organizationId: string;
  aggregateId: string;
  payload: Prisma.JsonValue;
  attemptCount: number;
  processingStartedAt: Date | null;
  lastError: Prisma.JsonValue | null;
};

export type ClaimedWebhookDelivery = {
  outboxEventId: string;
  organizationId: string;
  serviceRequestId: string;
  payload: Prisma.JsonValue;
  claimStartedAt: string;
  expectedAttemptNumber: number;
};

export type ClaimDueWebhookDeliveriesResult = {
  claimed: ClaimedWebhookDelivery[];
  recoveredStaleClaims: number;
  abandonedAttempts: number;
  exhausted: number;
};

export type ClaimDueWebhookDeliveriesOptions = {
  database: DatabaseClient;
  batchSize: number;
  claimStartedAt: Date;
  staleBefore: Date;
  maxAttempts: number;
  baseDelayMs?: number;
  maxDelayMs?: number;
  random?: () => number;
  afterRowsLocked?: (outboxEventIds: readonly string[]) => Promise<void>;
};

export type ReleaseWebhookDeliveryClaimOptions = {
  database: DatabaseClient;
  outboxEventId: string;
  organizationId: string;
  claimStartedAt: string;
  expectedAttemptNumber: number;
  retryAt: Date;
  error: Prisma.InputJsonObject;
};

export type ClaimedExhaustedWebhookDelivery = {
  outboxEventId: string;
  organizationId: string;
  serviceRequestId: string;
  payload: Prisma.JsonValue;
  attemptCount: number;
  claimStartedAt: string;
  lastError: Prisma.JsonValue | null;
};

export type ClaimExhaustedWebhookDeliveriesResult = {
  claimed: ClaimedExhaustedWebhookDelivery[];
  recoveredStaleClaims: number;
};

export type ClaimExhaustedWebhookDeliveriesOptions = {
  database: DatabaseClient;
  batchSize: number;
  claimStartedAt: Date;
  staleBefore: Date;
  afterRowsLocked?: (outboxEventIds: readonly string[]) => Promise<void>;
};

export type ExhaustedWebhookDeliveryDeadLetterClaimOptions = {
  database: DatabaseClient;
  outboxEventId: string;
  organizationId: string;
  claimStartedAt: string;
};

export type CompleteExhaustedWebhookDeliveryDeadLetterClaimOptions =
  ExhaustedWebhookDeliveryDeadLetterClaimOptions & {
    completedAt: Date;
  };

function assertPositiveInteger(value: number, name: string): void {
  if (!Number.isInteger(value) || value < 1) {
    throw new Error(`${name} must be a positive integer`);
  }
}

function assertValidDate(value: Date, name: string): void {
  if (Number.isNaN(value.getTime())) {
    throw new Error(`${name} must be a valid Date`);
  }
}

function parseClaimStartedAt(value: string): Date {
  const claimStartedAt = new Date(value);

  assertValidDate(claimStartedAt, "claimStartedAt");

  return claimStartedAt;
}

function createAbandonedAttemptError(
  recoveredAt: Date,
): Prisma.InputJsonObject {
  return {
    code: "ABANDONED_DELIVERY_ATTEMPT",
    message:
      "The delivery attempt was still STARTED after its PostgreSQL claim became stale; its external outcome is unknown",
    outcome: "unknown",
    recoveredAt: recoveredAt.toISOString(),
  };
}

function validateClaimWindow(claimStartedAt: Date, staleBefore: Date): void {
  assertValidDate(claimStartedAt, "claimStartedAt");
  assertValidDate(staleBefore, "staleBefore");

  if (staleBefore.getTime() >= claimStartedAt.getTime()) {
    throw new Error("staleBefore must be earlier than claimStartedAt");
  }
}

export async function claimDueWebhookDeliveries(
  options: ClaimDueWebhookDeliveriesOptions,
): Promise<ClaimDueWebhookDeliveriesResult> {
  assertPositiveInteger(options.batchSize, "batchSize");
  assertPositiveInteger(options.maxAttempts, "maxAttempts");
  validateClaimWindow(options.claimStartedAt, options.staleBefore);

  const baseDelayMs =
    options.baseDelayMs ?? DEFAULT_WEBHOOK_DELIVERY_BASE_DELAY_MS;
  const maxDelayMs =
    options.maxDelayMs ?? DEFAULT_WEBHOOK_DELIVERY_MAX_DELAY_MS;
  const random = options.random ?? Math.random;

  assertPositiveInteger(baseDelayMs, "baseDelayMs");
  assertPositiveInteger(maxDelayMs, "maxDelayMs");

  if (maxDelayMs < baseDelayMs) {
    throw new Error("maxDelayMs must be greater than or equal to baseDelayMs");
  }

  return options.database.$transaction(async (transaction) => {
    const lockedRows = await transaction.$queryRaw<LockedOutboxEventRow[]>(
      Prisma.sql`
        SELECT
          outbox_event.id AS "outboxEventId",
          outbox_event.organization_id AS "organizationId",
          outbox_event.aggregate_id AS "aggregateId",
          outbox_event.payload,
          outbox_event.status,
          outbox_event.attempt_count AS "attemptCount",
          outbox_event.processing_started_at AS "processingStartedAt"
        FROM outbox_events AS outbox_event
        WHERE outbox_event.event_type = ${ASSIGNED_EVENT_TYPE}
          AND (
            (
              outbox_event.status = 'PENDING'::"OutboxEventStatus"
              AND outbox_event.next_attempt_at <= ${options.claimStartedAt}
            )
            OR
            (
              outbox_event.status = 'PROCESSING'::"OutboxEventStatus"
              AND (
                outbox_event.processing_started_at IS NULL
                OR outbox_event.processing_started_at <= ${options.staleBefore}
              )
            )
          )
        ORDER BY
          CASE
            WHEN outbox_event.status = 'PENDING'::"OutboxEventStatus"
              THEN outbox_event.next_attempt_at
            ELSE outbox_event.processing_started_at
          END ASC,
          outbox_event.created_at ASC,
          outbox_event.id ASC
        LIMIT ${options.batchSize}
        FOR UPDATE OF outbox_event SKIP LOCKED
      `,
    );

    await options.afterRowsLocked?.(lockedRows.map((row) => row.outboxEventId));

    const claimed: ClaimedWebhookDelivery[] = [];
    let recoveredStaleClaims = 0;
    let abandonedAttempts = 0;
    let exhausted = 0;

    for (const row of lockedRows) {
      let abandonedAttemptError: Prisma.InputJsonObject | undefined;

      if (row.status === "PROCESSING") {
        recoveredStaleClaims += 1;

        const startedAttempts = await transaction.webhookDelivery.findMany({
          where: {
            organizationId: row.organizationId,
            outboxEventId: row.outboxEventId,
            status: "STARTED",
          },
          select: {
            id: true,
          },
        });

        if (startedAttempts.length > 0) {
          abandonedAttemptError = createAbandonedAttemptError(
            options.claimStartedAt,
          );

          const failedAttempts = await transaction.webhookDelivery.updateMany({
            where: {
              id: {
                in: startedAttempts.map((attempt) => attempt.id),
              },
              status: "STARTED",
            },
            data: {
              status: "FAILED",
              completedAt: options.claimStartedAt,
              errorDetails: abandonedAttemptError,
            },
          });

          abandonedAttempts += failedAttempts.count;

          if (row.attemptCount >= options.maxAttempts) {
            await transaction.outboxEvent.update({
              where: {
                id: row.outboxEventId,
              },
              data: {
                status: "EXHAUSTED",
                processingStartedAt: null,
                processedAt: null,
                nextAttemptAt: options.claimStartedAt,
                lastError: abandonedAttemptError,
              },
            });

            exhausted += 1;

            continue;
          }

          const { delayMs } = calculateFullJitterDelay({
            retryIndex: row.attemptCount - 1,
            baseDelayMs,
            maxDelayMs,
            randomValue: random(),
          });

          await transaction.outboxEvent.update({
            where: {
              id: row.outboxEventId,
            },
            data: {
              status: "PENDING",
              processingStartedAt: null,
              processedAt: null,
              nextAttemptAt: new Date(
                options.claimStartedAt.getTime() + delayMs,
              ),
              lastError: abandonedAttemptError,
            },
          });

          continue;
        }
      }

      await transaction.outboxEvent.update({
        where: {
          id: row.outboxEventId,
        },
        data: {
          status: "PROCESSING",
          processingStartedAt: options.claimStartedAt,
          processedAt: null,
          ...(abandonedAttemptError
            ? {
                lastError: abandonedAttemptError,
              }
            : {}),
        },
      });

      claimed.push({
        outboxEventId: row.outboxEventId,
        organizationId: row.organizationId,
        serviceRequestId: row.aggregateId,
        payload: row.payload,
        claimStartedAt: options.claimStartedAt.toISOString(),
        expectedAttemptNumber: row.attemptCount + 1,
      });
    }

    return {
      claimed,
      recoveredStaleClaims,
      abandonedAttempts,
      exhausted,
    };
  });
}

export async function exhaustInvalidWebhookDeliveryClaim(options: {
  database: DatabaseClient;
  outboxEventId: string;
  organizationId: string;
  claimStartedAt: string;
  expectedAttemptNumber: number;
  exhaustedAt: Date;
  error: Prisma.InputJsonObject;
}): Promise<boolean> {
  assertPositiveInteger(options.expectedAttemptNumber, "expectedAttemptNumber");
  assertValidDate(options.exhaustedAt, "exhaustedAt");

  const claimStartedAt = parseClaimStartedAt(options.claimStartedAt);

  const exhausted = await options.database.outboxEvent.updateMany({
    where: {
      id: options.outboxEventId,
      organizationId: options.organizationId,
      status: "PROCESSING",
      processingStartedAt: claimStartedAt,
      attemptCount: options.expectedAttemptNumber - 1,
    },
    data: {
      status: "EXHAUSTED",
      processingStartedAt: null,
      processedAt: null,
      nextAttemptAt: options.exhaustedAt,
      lastError: options.error,
    },
  });

  return exhausted.count === 1;
}

export async function releaseWebhookDeliveryClaim(
  options: ReleaseWebhookDeliveryClaimOptions,
): Promise<boolean> {
  assertPositiveInteger(options.expectedAttemptNumber, "expectedAttemptNumber");
  assertValidDate(options.retryAt, "retryAt");

  const claimStartedAt = parseClaimStartedAt(options.claimStartedAt);

  const released = await options.database.outboxEvent.updateMany({
    where: {
      id: options.outboxEventId,
      organizationId: options.organizationId,
      status: "PROCESSING",
      processingStartedAt: claimStartedAt,
      attemptCount: options.expectedAttemptNumber - 1,
    },
    data: {
      status: "PENDING",
      processingStartedAt: null,
      processedAt: null,
      nextAttemptAt: options.retryAt,
      lastError: options.error,
    },
  });

  return released.count === 1;
}

export async function claimExhaustedWebhookDeliveriesForDeadLetter(
  options: ClaimExhaustedWebhookDeliveriesOptions,
): Promise<ClaimExhaustedWebhookDeliveriesResult> {
  assertPositiveInteger(options.batchSize, "batchSize");
  validateClaimWindow(options.claimStartedAt, options.staleBefore);

  return options.database.$transaction(async (transaction) => {
    const lockedRows = await transaction.$queryRaw<
      LockedExhaustedOutboxEventRow[]
    >(Prisma.sql`
      SELECT
        outbox_event.id AS "outboxEventId",
        outbox_event.organization_id AS "organizationId",
        outbox_event.aggregate_id AS "aggregateId",
        outbox_event.payload,
        outbox_event.attempt_count AS "attemptCount",
        outbox_event.processing_started_at AS "processingStartedAt",
        outbox_event.last_error AS "lastError"
      FROM outbox_events AS outbox_event
      WHERE outbox_event.event_type = ${ASSIGNED_EVENT_TYPE}
        AND outbox_event.status = 'EXHAUSTED'::"OutboxEventStatus"
        AND outbox_event.processed_at IS NULL
        AND (
          outbox_event.processing_started_at IS NULL
          OR outbox_event.processing_started_at <= ${options.staleBefore}
        )
      ORDER BY
        outbox_event.updated_at ASC,
        outbox_event.created_at ASC,
        outbox_event.id ASC
      LIMIT ${options.batchSize}
      FOR UPDATE OF outbox_event SKIP LOCKED
    `);

    await options.afterRowsLocked?.(lockedRows.map((row) => row.outboxEventId));

    for (const row of lockedRows) {
      await transaction.outboxEvent.update({
        where: {
          id: row.outboxEventId,
        },
        data: {
          processingStartedAt: options.claimStartedAt,
        },
      });
    }

    return {
      claimed: lockedRows.map((row) => ({
        outboxEventId: row.outboxEventId,
        organizationId: row.organizationId,
        serviceRequestId: row.aggregateId,
        payload: row.payload,
        attemptCount: row.attemptCount,
        claimStartedAt: options.claimStartedAt.toISOString(),
        lastError: row.lastError,
      })),
      recoveredStaleClaims: lockedRows.filter(
        (row) => row.processingStartedAt !== null,
      ).length,
    };
  });
}

export async function releaseExhaustedWebhookDeliveryDeadLetterClaim(
  options: ExhaustedWebhookDeliveryDeadLetterClaimOptions,
): Promise<boolean> {
  const claimStartedAt = parseClaimStartedAt(options.claimStartedAt);

  const released = await options.database.outboxEvent.updateMany({
    where: {
      id: options.outboxEventId,
      organizationId: options.organizationId,
      status: "EXHAUSTED",
      processedAt: null,
      processingStartedAt: claimStartedAt,
    },
    data: {
      processingStartedAt: null,
    },
  });

  return released.count === 1;
}

export async function completeExhaustedWebhookDeliveryDeadLetterClaim(
  options: CompleteExhaustedWebhookDeliveryDeadLetterClaimOptions,
): Promise<boolean> {
  assertValidDate(options.completedAt, "completedAt");

  const claimStartedAt = parseClaimStartedAt(options.claimStartedAt);

  const completed = await options.database.outboxEvent.updateMany({
    where: {
      id: options.outboxEventId,
      organizationId: options.organizationId,
      status: "EXHAUSTED",
      processedAt: null,
      processingStartedAt: claimStartedAt,
    },
    data: {
      processingStartedAt: null,
      processedAt: options.completedAt,
    },
  });

  return completed.count === 1;
}
