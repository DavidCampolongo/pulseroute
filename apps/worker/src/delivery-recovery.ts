import { Prisma, type DatabaseClient } from "@pulseroute/db";
import { QUEUE_NAMES, type DeadLetteredJobData } from "@pulseroute/shared";

type LockedRecoveryEventRow = {
  outboxEventId: string;
  status: "PENDING" | "PROCESSING" | "DELIVERED" | "EXHAUSTED";
  attemptCount: number;
};

export type RecoverWebhookDeliveryResult =
  | {
      kind: "recovered";
      outboxEventId: string;
      attemptCount: number;
      nextExpectedAttemptNumber: number;
      nextAttemptAt: string;
    }
  | {
      kind: "stale_recovery_generation";
      outboxEventId: string;
      deadLetterAttemptCount: number;
      currentAttemptCount: number;
    }
  | {
      kind: "already_recovered_or_terminal";
      outboxEventId: string;
      status: Exclude<LockedRecoveryEventRow["status"], "EXHAUSTED">;
      attemptCount: number;
    };

export type RecoverWebhookDeliveryOptions = {
  database: DatabaseClient;
  outboxEventId: string;
  organizationId: string;
  recoveredAt: Date;
  expectedAttemptCount?: number;
};

function assertValidDate(value: Date): void {
  if (Number.isNaN(value.getTime())) {
    throw new Error("recoveredAt must be a valid Date");
  }
}

function assertNonnegativeInteger(value: number, name: string): void {
  if (!Number.isInteger(value) || value < 0) {
    throw new Error(`${name} must be a nonnegative integer`);
  }
}

export async function recoverExhaustedWebhookDelivery(
  options: RecoverWebhookDeliveryOptions,
): Promise<RecoverWebhookDeliveryResult> {
  assertValidDate(options.recoveredAt);
  if (options.expectedAttemptCount !== undefined) {
    assertNonnegativeInteger(
      options.expectedAttemptCount,
      "expectedAttemptCount",
    );
  }

  return options.database.$transaction(async (transaction) => {
    const lockedRows = await transaction.$queryRaw<LockedRecoveryEventRow[]>(
      Prisma.sql`
        SELECT
          outbox_event.id AS "outboxEventId",
          outbox_event.status,
          outbox_event.attempt_count AS "attemptCount"
        FROM outbox_events AS outbox_event
        WHERE outbox_event.id = ${options.outboxEventId}::uuid
          AND outbox_event.organization_id = ${options.organizationId}::uuid
          AND outbox_event.event_type = 'service_request.assigned'
        FOR UPDATE OF outbox_event
      `,
    );

    const event = lockedRows[0];

    if (!event) {
      throw new Error("Exhausted assigned OutboxEvent does not exist");
    }

    if (event.status !== "EXHAUSTED") {
      return {
        kind: "already_recovered_or_terminal" as const,
        outboxEventId: event.outboxEventId,
        status: event.status,
        attemptCount: event.attemptCount,
      };
    }

    if (
      options.expectedAttemptCount !== undefined &&
      event.attemptCount !== options.expectedAttemptCount
    ) {
      return {
        kind: "stale_recovery_generation" as const,
        outboxEventId: event.outboxEventId,
        deadLetterAttemptCount: options.expectedAttemptCount,
        currentAttemptCount: event.attemptCount,
      };
    }

    await transaction.outboxEvent.update({
      where: {
        id: event.outboxEventId,
      },
      data: {
        status: "PENDING",
        processingStartedAt: null,
        processedAt: null,
        nextAttemptAt: options.recoveredAt,
      },
    });

    return {
      kind: "recovered" as const,
      outboxEventId: event.outboxEventId,
      attemptCount: event.attemptCount,
      nextExpectedAttemptNumber: event.attemptCount + 1,
      nextAttemptAt: options.recoveredAt.toISOString(),
    };
  });
}

export async function recoverDeadLetteredWebhookDelivery(options: {
  database: DatabaseClient;
  deadLetter: DeadLetteredJobData;
  recoveredAt: Date;
}): Promise<RecoverWebhookDeliveryResult> {
  if (options.deadLetter.sourceQueue !== QUEUE_NAMES.webhookDelivery) {
    throw new Error("Dead-letter job is not for webhook delivery");
  }

  if (!options.deadLetter.outboxEventId) {
    throw new Error("Webhook delivery dead-letter job has no outboxEventId");
  }

  if (!options.deadLetter.organizationId) {
    throw new Error("Webhook delivery dead-letter job has no organizationId");
  }

  return recoverExhaustedWebhookDelivery({
    database: options.database,
    outboxEventId: options.deadLetter.outboxEventId,
    organizationId: options.deadLetter.organizationId,
    recoveredAt: options.recoveredAt,
    expectedAttemptCount: options.deadLetter.attemptsMade,
  });
}
