import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";

import { createDatabaseClient } from "@pulseroute/db";
import { config as loadEnvironmentFile } from "dotenv";
import { afterAll, afterEach, describe, expect, it } from "vitest";

import {
  claimDueWebhookDeliveries,
  claimExhaustedWebhookDeliveriesForDeadLetter,
  completeExhaustedWebhookDeliveryDeadLetterClaim,
  releaseExhaustedWebhookDeliveryDeadLetterClaim,
  releaseWebhookDeliveryClaim,
} from "../src/delivery-claimer.js";

loadEnvironmentFile({
  path: fileURLToPath(new URL("../../../.env", import.meta.url)),
  quiet: true,
});

const databaseUrl = process.env.DATABASE_URL;

if (!databaseUrl) {
  throw new Error("DATABASE_URL is required for delivery claimer tests");
}

const fixtureDatabase = createDatabaseClient(databaseUrl);
const claimerADatabase = createDatabaseClient(databaseUrl);
const claimerBDatabase = createDatabaseClient(databaseUrl);

const CREATED_AT = new Date("1899-12-31T23:00:00.000Z");
const FIRST_DUE_AT = new Date("1899-12-31T23:10:00.000Z");
const SECOND_DUE_AT = new Date("1899-12-31T23:20:00.000Z");
const OLD_CLAIM_AT = new Date("1899-12-31T23:30:00.000Z");
const STALE_BEFORE = new Date("1899-12-31T23:45:00.000Z");
const CLAIM_A_AT = new Date("1900-01-01T00:00:00.000Z");
const RECOVERED_RETRY_AT = new Date("1900-01-01T00:00:00.500Z");
const CLAIM_B_AT = new Date("1900-01-01T00:00:01.000Z");
const RETRY_AT = new Date("1900-01-01T00:05:00.000Z");
const DEAD_LETTER_COMPLETE_AT = new Date("1900-01-01T00:10:00.000Z");

const organizationIds: string[] = [];

type OutboxFixtureOptions = {
  organizationId: string;
  nextAttemptAt?: Date;
  processingStartedAt?: Date | null;
  status?: "PENDING" | "PROCESSING" | "EXHAUSTED";
  attemptCount?: number;
  processedAt?: Date | null;
};

type OutboxFixture = {
  outboxEventId: string;
  organizationId: string;
  serviceRequestId: string;
  correlationId: string;
};

async function createOrganization(): Promise<string> {
  const organizationId = randomUUID();

  await fixtureDatabase.organization.create({
    data: {
      id: organizationId,
      name: `Delivery Claimer Test ${organizationId}`,
    },
  });

  organizationIds.push(organizationId);

  return organizationId;
}

async function createOutboxFixture(
  options: OutboxFixtureOptions,
): Promise<OutboxFixture> {
  const outboxEventId = randomUUID();
  const serviceRequestId = randomUUID();
  const correlationId = `delivery-claim-${randomUUID()}`;

  await fixtureDatabase.outboxEvent.create({
    data: {
      id: outboxEventId,
      organizationId: options.organizationId,
      eventType: "service_request.assigned",
      aggregateType: "service_request",
      aggregateId: serviceRequestId,
      status: options.status ?? "PENDING",
      payload: {
        organizationId: options.organizationId,
        serviceRequestId,
        correlationId,
      },
      attemptCount: options.attemptCount ?? 0,
      nextAttemptAt: options.nextAttemptAt ?? FIRST_DUE_AT,
      processingStartedAt: options.processingStartedAt,
      processedAt: options.processedAt,
      createdAt: CREATED_AT,
    },
  });

  return {
    outboxEventId,
    organizationId: options.organizationId,
    serviceRequestId,
    correlationId,
  };
}

async function clearFixtures(): Promise<void> {
  const fixtureOrganizationIds = organizationIds.splice(0);

  if (fixtureOrganizationIds.length === 0) {
    return;
  }

  await fixtureDatabase.webhookDelivery.deleteMany({
    where: {
      organizationId: {
        in: fixtureOrganizationIds,
      },
    },
  });

  await fixtureDatabase.outboxEvent.deleteMany({
    where: {
      organizationId: {
        in: fixtureOrganizationIds,
      },
    },
  });

  await fixtureDatabase.organization.deleteMany({
    where: {
      id: {
        in: fixtureOrganizationIds,
      },
    },
  });
}

function createTimeout(milliseconds: number): Promise<never> {
  return new Promise((_, reject) => {
    setTimeout(() => {
      reject(new Error("Concurrent SKIP LOCKED claim timed out"));
    }, milliseconds).unref();
  });
}

afterEach(async () => {
  await clearFixtures();
});

afterAll(async () => {
  await clearFixtures();

  await Promise.all([
    fixtureDatabase.$disconnect(),
    claimerADatabase.$disconnect(),
    claimerBDatabase.$disconnect(),
  ]);
});

describe("delivery claimer", () => {
  it("lets two real PostgreSQL claimers lock different due rows without waiting", async () => {
    const organizationId = await createOrganization();
    const first = await createOutboxFixture({
      organizationId,
      nextAttemptAt: FIRST_DUE_AT,
    });
    const second = await createOutboxFixture({
      organizationId,
      nextAttemptAt: SECOND_DUE_AT,
    });

    let reportFirstRowsLocked = (): void => undefined;
    let releaseFirstClaim = (): void => undefined;

    const firstRowsLocked = new Promise<void>((resolve) => {
      reportFirstRowsLocked = resolve;
    });

    const firstClaimReleased = new Promise<void>((resolve) => {
      releaseFirstClaim = resolve;
    });

    const firstClaimPromise = claimDueWebhookDeliveries({
      database: claimerADatabase,
      batchSize: 1,
      claimStartedAt: CLAIM_A_AT,
      staleBefore: STALE_BEFORE,
      maxAttempts: 3,
      afterRowsLocked: async (outboxEventIds) => {
        expect(outboxEventIds).toEqual([first.outboxEventId]);

        reportFirstRowsLocked();

        await firstClaimReleased;
      },
    });

    await firstRowsLocked;

    try {
      const secondClaim = await Promise.race([
        claimDueWebhookDeliveries({
          database: claimerBDatabase,
          batchSize: 1,
          claimStartedAt: CLAIM_B_AT,
          staleBefore: STALE_BEFORE,
          maxAttempts: 3,
        }),
        createTimeout(2_000),
      ]);

      expect(secondClaim.claimed).toHaveLength(1);
      expect(secondClaim.claimed[0]).toMatchObject({
        outboxEventId: second.outboxEventId,
        organizationId,
        serviceRequestId: second.serviceRequestId,
        claimStartedAt: CLAIM_B_AT.toISOString(),
        expectedAttemptNumber: 1,
      });

      releaseFirstClaim();

      const firstClaim = await firstClaimPromise;

      expect(firstClaim.claimed).toHaveLength(1);
      expect(firstClaim.claimed[0]).toMatchObject({
        outboxEventId: first.outboxEventId,
        claimStartedAt: CLAIM_A_AT.toISOString(),
        expectedAttemptNumber: 1,
      });

      expect(
        new Set(
          [...firstClaim.claimed, ...secondClaim.claimed].map(
            (claim) => claim.outboxEventId,
          ),
        ),
      ).toEqual(new Set([first.outboxEventId, second.outboxEventId]));

      const thirdClaim = await claimDueWebhookDeliveries({
        database: fixtureDatabase,
        batchSize: 2,
        claimStartedAt: new Date("1900-01-01T00:00:02.000Z"),
        staleBefore: STALE_BEFORE,
        maxAttempts: 3,
      });

      expect(thirdClaim.claimed).toEqual([]);
    } finally {
      releaseFirstClaim();

      await firstClaimPromise;
    }
  }, 10_000);

  it("durably jitters a stale STARTED attempt before reclaiming its next attempt", async () => {
    const organizationId = await createOrganization();
    const fixture = await createOutboxFixture({
      organizationId,
      status: "PROCESSING",
      processingStartedAt: OLD_CLAIM_AT,
      attemptCount: 1,
    });
    const deliveryId = randomUUID();

    await fixtureDatabase.webhookDelivery.create({
      data: {
        id: deliveryId,
        organizationId,
        outboxEventId: fixture.outboxEventId,
        attemptNumber: 1,
        status: "STARTED",
        startedAt: OLD_CLAIM_AT,
      },
    });

    const result = await claimDueWebhookDeliveries({
      database: claimerADatabase,
      batchSize: 1,
      claimStartedAt: CLAIM_A_AT,
      staleBefore: STALE_BEFORE,
      maxAttempts: 3,
      baseDelayMs: 1_000,
      maxDelayMs: 1_000,
      random: () => 0.5,
    });

    expect(result).toMatchObject({
      claimed: [],
      recoveredStaleClaims: 1,
      abandonedAttempts: 1,
      exhausted: 0,
    });

    const scheduledOutbox = await fixtureDatabase.outboxEvent.findUniqueOrThrow(
      {
        where: {
          id: fixture.outboxEventId,
        },
      },
    );

    expect(scheduledOutbox).toMatchObject({
      status: "PENDING",
      attemptCount: 1,
      processingStartedAt: null,
      processedAt: null,
      nextAttemptAt: RECOVERED_RETRY_AT,
      lastError: {
        code: "ABANDONED_DELIVERY_ATTEMPT",
        outcome: "unknown",
      },
    });

    const prematureClaim = await claimDueWebhookDeliveries({
      database: claimerBDatabase,
      batchSize: 1,
      claimStartedAt: CLAIM_A_AT,
      staleBefore: STALE_BEFORE,
      maxAttempts: 3,
    });

    expect(prematureClaim.claimed).toEqual([]);

    const dueClaim = await claimDueWebhookDeliveries({
      database: claimerBDatabase,
      batchSize: 1,
      claimStartedAt: RECOVERED_RETRY_AT,
      staleBefore: STALE_BEFORE,
      maxAttempts: 3,
    });

    expect(dueClaim.claimed).toHaveLength(1);
    expect(dueClaim.claimed[0]).toMatchObject({
      outboxEventId: fixture.outboxEventId,
      expectedAttemptNumber: 2,
      claimStartedAt: RECOVERED_RETRY_AT.toISOString(),
    });

    const abandonedDelivery =
      await fixtureDatabase.webhookDelivery.findUniqueOrThrow({
        where: {
          id: deliveryId,
        },
      });

    expect(abandonedDelivery.status).toBe("FAILED");
    expect(abandonedDelivery.completedAt).toEqual(CLAIM_A_AT);
    expect(abandonedDelivery.errorDetails).toMatchObject({
      code: "ABANDONED_DELIVERY_ATTEMPT",
      outcome: "unknown",
    });

    const oldClaimRelease = await releaseWebhookDeliveryClaim({
      database: fixtureDatabase,
      outboxEventId: fixture.outboxEventId,
      organizationId,
      claimStartedAt: OLD_CLAIM_AT.toISOString(),
      expectedAttemptNumber: 2,
      retryAt: RETRY_AT,
      error: {
        code: "QUEUE_PUBLICATION_FAILED",
      },
    });

    expect(oldClaimRelease).toBe(false);

    const currentClaimRelease = await releaseWebhookDeliveryClaim({
      database: fixtureDatabase,
      outboxEventId: fixture.outboxEventId,
      organizationId,
      claimStartedAt: RECOVERED_RETRY_AT.toISOString(),
      expectedAttemptNumber: 2,
      retryAt: RETRY_AT,
      error: {
        code: "QUEUE_PUBLICATION_FAILED",
      },
    });

    expect(currentClaimRelease).toBe(true);

    const releasedOutbox = await fixtureDatabase.outboxEvent.findUniqueOrThrow({
      where: {
        id: fixture.outboxEventId,
      },
    });

    expect(releasedOutbox).toMatchObject({
      status: "PENDING",
      attemptCount: 1,
      processingStartedAt: null,
      processedAt: null,
      nextAttemptAt: RETRY_AT,
    });
    expect(releasedOutbox.lastError).toEqual({
      code: "QUEUE_PUBLICATION_FAILED",
    });
  });

  it("exhausts an abandoned final attempt and publishes its DLQ lease once", async () => {
    const organizationId = await createOrganization();
    const fixture = await createOutboxFixture({
      organizationId,
      status: "PROCESSING",
      processingStartedAt: OLD_CLAIM_AT,
      attemptCount: 3,
    });

    await fixtureDatabase.webhookDelivery.create({
      data: {
        organizationId,
        outboxEventId: fixture.outboxEventId,
        attemptNumber: 3,
        status: "STARTED",
        startedAt: OLD_CLAIM_AT,
      },
    });

    const recovery = await claimDueWebhookDeliveries({
      database: claimerADatabase,
      batchSize: 1,
      claimStartedAt: CLAIM_A_AT,
      staleBefore: STALE_BEFORE,
      maxAttempts: 3,
    });

    expect(recovery).toMatchObject({
      claimed: [],
      recoveredStaleClaims: 1,
      abandonedAttempts: 1,
      exhausted: 1,
    });

    const exhaustedOutbox = await fixtureDatabase.outboxEvent.findUniqueOrThrow(
      {
        where: {
          id: fixture.outboxEventId,
        },
      },
    );

    expect(exhaustedOutbox).toMatchObject({
      status: "EXHAUSTED",
      attemptCount: 3,
      processingStartedAt: null,
      processedAt: null,
    });

    const deadLetterClaim = await claimExhaustedWebhookDeliveriesForDeadLetter({
      database: claimerADatabase,
      batchSize: 1,
      claimStartedAt: CLAIM_B_AT,
      staleBefore: STALE_BEFORE,
    });

    expect(deadLetterClaim.claimed).toHaveLength(1);
    expect(deadLetterClaim.claimed[0]).toMatchObject({
      outboxEventId: fixture.outboxEventId,
      serviceRequestId: fixture.serviceRequestId,
      attemptCount: 3,
      claimStartedAt: CLAIM_B_AT.toISOString(),
      lastError: {
        code: "ABANDONED_DELIVERY_ATTEMPT",
      },
    });

    const duplicateDeadLetterClaim =
      await claimExhaustedWebhookDeliveriesForDeadLetter({
        database: claimerBDatabase,
        batchSize: 1,
        claimStartedAt: new Date("1900-01-01T00:00:02.000Z"),
        staleBefore: STALE_BEFORE,
      });

    expect(duplicateDeadLetterClaim.claimed).toEqual([]);

    expect(
      await releaseExhaustedWebhookDeliveryDeadLetterClaim({
        database: fixtureDatabase,
        outboxEventId: fixture.outboxEventId,
        organizationId,
        claimStartedAt: OLD_CLAIM_AT.toISOString(),
      }),
    ).toBe(false);

    expect(
      await completeExhaustedWebhookDeliveryDeadLetterClaim({
        database: fixtureDatabase,
        outboxEventId: fixture.outboxEventId,
        organizationId,
        claimStartedAt: CLAIM_B_AT.toISOString(),
        completedAt: DEAD_LETTER_COMPLETE_AT,
      }),
    ).toBe(true);

    const completedOutbox = await fixtureDatabase.outboxEvent.findUniqueOrThrow(
      {
        where: {
          id: fixture.outboxEventId,
        },
      },
    );

    expect(completedOutbox).toMatchObject({
      status: "EXHAUSTED",
      processingStartedAt: null,
      processedAt: DEAD_LETTER_COMPLETE_AT,
    });
  });

  it("releases an exhausted DLQ claim only for the current version", async () => {
    const organizationId = await createOrganization();
    const fixture = await createOutboxFixture({
      organizationId,
      status: "EXHAUSTED",
      attemptCount: 3,
      processedAt: null,
    });

    const claimed = await claimExhaustedWebhookDeliveriesForDeadLetter({
      database: fixtureDatabase,
      batchSize: 1,
      claimStartedAt: CLAIM_A_AT,
      staleBefore: STALE_BEFORE,
    });

    expect(claimed.claimed).toHaveLength(1);

    expect(
      await releaseExhaustedWebhookDeliveryDeadLetterClaim({
        database: fixtureDatabase,
        outboxEventId: fixture.outboxEventId,
        organizationId,
        claimStartedAt: CLAIM_A_AT.toISOString(),
      }),
    ).toBe(true);

    const released = await fixtureDatabase.outboxEvent.findUniqueOrThrow({
      where: {
        id: fixture.outboxEventId,
      },
    });

    expect(released).toMatchObject({
      status: "EXHAUSTED",
      processingStartedAt: null,
      processedAt: null,
    });
  });

  it("rejects unsafe claim bounds before querying PostgreSQL", async () => {
    await expect(
      claimDueWebhookDeliveries({
        database: fixtureDatabase,
        batchSize: 0,
        claimStartedAt: CLAIM_A_AT,
        staleBefore: STALE_BEFORE,
        maxAttempts: 3,
      }),
    ).rejects.toThrow("batchSize must be a positive integer");

    await expect(
      claimDueWebhookDeliveries({
        database: fixtureDatabase,
        batchSize: 1,
        claimStartedAt: CLAIM_A_AT,
        staleBefore: STALE_BEFORE,
        maxAttempts: 0,
      }),
    ).rejects.toThrow("maxAttempts must be a positive integer");

    await expect(
      claimExhaustedWebhookDeliveriesForDeadLetter({
        database: fixtureDatabase,
        batchSize: 1,
        claimStartedAt: CLAIM_A_AT,
        staleBefore: CLAIM_A_AT,
      }),
    ).rejects.toThrow("staleBefore must be earlier than claimStartedAt");
  });
});
