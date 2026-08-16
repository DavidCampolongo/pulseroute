import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";

import { createDatabaseClient } from "@pulseroute/db";
import {
  JOB_NAMES,
  type DeadLetteredJobData,
  type WebhookDeliveryJobData,
} from "@pulseroute/shared";
import { Queue } from "bullmq";
import { config as loadEnvironmentFile } from "dotenv";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";

import { DeliveryScheduler } from "../src/delivery-scheduler.js";
import { readDatabaseNow } from "../src/database-clock.js";
import { claimDueWebhookDeliveries } from "../src/delivery-claimer.js";
import { createWorkerLogger } from "../src/logger.js";
import { createProducerRedisOptions } from "../src/redis.js";

loadEnvironmentFile({
  path: fileURLToPath(new URL("../../../.env", import.meta.url)),
  quiet: true,
});

const databaseUrl = process.env.DATABASE_URL;
const redisUrl = process.env.REDIS_URL;

if (!databaseUrl) {
  throw new Error("DATABASE_URL is required for delivery scheduler tests");
}

if (!redisUrl) {
  throw new Error("REDIS_URL is required for delivery scheduler tests");
}
const concurrentClaimerADatabase = createDatabaseClient(databaseUrl);
const concurrentClaimerBDatabase = createDatabaseClient(databaseUrl);

const database = createDatabaseClient(databaseUrl);
const deliveryQueue = new Queue<WebhookDeliveryJobData>(
  `phase9-scheduler-delivery-${randomUUID()}`,
  {
    connection: createProducerRedisOptions(redisUrl),
    skipWaitingForReady: true,
  },
);
const deadLetterQueue = new Queue<DeadLetteredJobData>(
  `phase9-scheduler-dlq-${randomUUID()}`,
  {
    connection: createProducerRedisOptions(redisUrl),
    skipWaitingForReady: true,
  },
);
const logger = createWorkerLogger({
  nodeEnv: "test",
  logLevel: "silent",
});
const NOW = new Date("1902-01-01T00:00:00.000Z");
const DUE_AT = new Date("1901-12-31T23:00:00.000Z");
const organizationIds: string[] = [];

async function createOutbox(
  status: "PENDING" | "EXHAUSTED",
  malformed = false,
) {
  const organizationId = randomUUID();
  const outboxEventId = randomUUID();
  const serviceRequestId = randomUUID();
  const correlationId = `delivery-scheduler-${randomUUID()}`;

  organizationIds.push(organizationId);

  await database.organization.create({
    data: {
      id: organizationId,
      name: `Delivery Scheduler ${organizationId}`,
    },
  });
  await database.outboxEvent.create({
    data: {
      id: outboxEventId,
      organizationId,
      eventType: "service_request.assigned",
      aggregateType: "service_request",
      aggregateId: serviceRequestId,
      status,
      attemptCount: status === "EXHAUSTED" ? 5 : 0,
      nextAttemptAt: DUE_AT,
      processedAt: null,
      payload: malformed
        ? {
            organizationId,
            serviceRequestId,
          }
        : {
            organizationId,
            serviceRequestId,
            operatorId: randomUUID(),
            assignmentId: randomUUID(),
            routingDecisionId: randomUUID(),
            scoringVersion: "pulseroute-scoring-v1",
            correlationId,
          },
      lastError:
        status === "EXHAUSTED"
          ? {
              code: "HTTP_ERROR",
              message: "Receiver returned HTTP 500",
              failedAt: DUE_AT.toISOString(),
            }
          : undefined,
      createdAt: DUE_AT,
    },
  });

  return {
    organizationId,
    outboxEventId,
    serviceRequestId,
    correlationId,
  };
}

async function clearFixtures(): Promise<void> {
  const ids = organizationIds.splice(0);

  if (ids.length === 0) {
    return;
  }

  await database.webhookDelivery.deleteMany({
    where: {
      organizationId: {
        in: ids,
      },
    },
  });
  await database.outboxEvent.deleteMany({
    where: {
      organizationId: {
        in: ids,
      },
    },
  });
  await database.organization.deleteMany({
    where: {
      id: {
        in: ids,
      },
    },
  });
}

function createConcurrencyTimeout(milliseconds: number): Promise<never> {
  return new Promise((_, reject) => {
    setTimeout(() => {
      reject(new Error("Concurrent claim-to-publication proof timed out"));
    }, milliseconds).unref();
  });
}

beforeAll(async () => {
  await Promise.all([
    deliveryQueue.waitUntilReady(),
    deadLetterQueue.waitUntilReady(),
  ]);
  await Promise.all([
    deliveryQueue.obliterate({ force: true }),
    deadLetterQueue.obliterate({ force: true }),
  ]);
});

afterEach(async () => {
  await Promise.all([
    deliveryQueue.obliterate({ force: true }),
    deadLetterQueue.obliterate({ force: true }),
  ]);
  await clearFixtures();
});

describe("delivery scheduler release proofs", () => {
  it("publishes one real Redis job from two overlapping PostgreSQL claimers", async () => {
    const due = await createOutbox("PENDING");
    const claimAAt = new Date(NOW);
    const claimBAt = new Date(NOW.getTime() + 1);
    const staleBefore = new Date(NOW.getTime() - 1_000);
    let reportFirstRowsLocked = (): void => undefined;
    let releaseFirstClaim = (): void => undefined;
    const firstRowsLocked = new Promise<void>((resolve) => {
      reportFirstRowsLocked = resolve;
    });
    const firstClaimReleased = new Promise<void>((resolve) => {
      releaseFirstClaim = resolve;
    });
    const firstClaimPromise = claimDueWebhookDeliveries({
      database: concurrentClaimerADatabase,
      batchSize: 1,
      claimStartedAt: claimAAt,
      staleBefore,
      maxAttempts: 5,
      afterRowsLocked: async (outboxEventIds) => {
        expect(outboxEventIds).toEqual([due.outboxEventId]);
        reportFirstRowsLocked();
        await firstClaimReleased;
      },
    });

    await firstRowsLocked;

    let secondClaim: Awaited<ReturnType<typeof claimDueWebhookDeliveries>>;

    try {
      secondClaim = await Promise.race([
        claimDueWebhookDeliveries({
          database: concurrentClaimerBDatabase,
          batchSize: 1,
          claimStartedAt: claimBAt,
          staleBefore,
          maxAttempts: 5,
        }),
        createConcurrencyTimeout(2_000),
      ]);
    } finally {
      releaseFirstClaim();
    }

    const firstClaim = await firstClaimPromise;

    expect(firstClaim.claimed).toHaveLength(1);
    expect(secondClaim.claimed).toEqual([]);

    const preparedClaims = [...firstClaim.claimed, ...secondClaim.claimed];

    for (const claim of preparedClaims) {
      const correlationId = (claim.payload as { correlationId?: unknown })
        .correlationId;

      if (typeof correlationId !== "string") {
        throw new Error("Claimed payload had no correlationId");
      }

      await deliveryQueue.add(
        JOB_NAMES.deliverWebhook,
        {
          outboxEventId: claim.outboxEventId,
          organizationId: claim.organizationId,
          correlationId,
          expectedAttemptNumber: claim.expectedAttemptNumber,
          claimStartedAt: claim.claimStartedAt,
        },
        {
          jobId: `concurrent-claim-${claim.outboxEventId}`,
          attempts: 1,
        },
      );
    }

    const [jobs, claimedOutbox] = await Promise.all([
      deliveryQueue.getJobs(["waiting"]),
      database.outboxEvent.findUniqueOrThrow({
        where: {
          id: due.outboxEventId,
        },
      }),
    ]);

    expect(jobs).toHaveLength(1);
    expect(jobs[0]).toMatchObject({
      name: JOB_NAMES.deliverWebhook,
      data: {
        outboxEventId: due.outboxEventId,
        expectedAttemptNumber: 1,
        claimStartedAt: claimAAt.toISOString(),
      },
      opts: {
        attempts: 1,
      },
    });
    expect(claimedOutbox).toMatchObject({
      status: "PROCESSING",
      attemptCount: 0,
      processingStartedAt: claimAAt,
    });
  }, 10_000);

  it("uses the default PostgreSQL clock for scheduler claim timestamps", async () => {
    const due = await createOutbox("PENDING");
    const databaseBefore = await readDatabaseNow(database);
    const scheduler = new DeliveryScheduler({
      database,
      webhookDeliveryQueue: deliveryQueue,
      deadLetterQueue,
      logger,
      pollIntervalMs: 10,
      batchSize: 1,
      claimTimeoutMs: 1_000,
      maxAttempts: 5,
    });

    const result = await scheduler.publishOnce();
    const databaseAfter = await readDatabaseNow(database);
    const jobs = await deliveryQueue.getJobs(["waiting"]);
    const claimedOutbox = await database.outboxEvent.findUniqueOrThrow({
      where: {
        id: due.outboxEventId,
      },
    });

    expect(result.deliveryClaimed).toBe(1);
    expect(result.deliveryPublished).toBe(1);
    expect(jobs).toHaveLength(1);

    const claimStartedAt = new Date(jobs[0]!.data.claimStartedAt);

    expect(claimStartedAt.getTime()).toBeGreaterThanOrEqual(
      databaseBefore.getTime(),
    );
    expect(claimStartedAt.getTime()).toBeLessThanOrEqual(
      databaseAfter.getTime(),
    );
    expect(claimStartedAt.getUTCFullYear()).not.toBe(NOW.getUTCFullYear());
    expect(claimedOutbox.processingStartedAt).toEqual(claimStartedAt);
  });
});

afterAll(async () => {
  await Promise.all([
    deliveryQueue.obliterate({ force: true }).catch(() => undefined),
    deadLetterQueue.obliterate({ force: true }).catch(() => undefined),
  ]);
  await Promise.all([deliveryQueue.close(), deadLetterQueue.close()]);
  await Promise.all([
    concurrentClaimerADatabase.$disconnect(),
    concurrentClaimerBDatabase.$disconnect(),
  ]);
  await clearFixtures();
  await database.$disconnect();
});

describe("delivery scheduler", () => {
  it("publishes claimed delivery work and crash-recoverable exhausted DLQ work", async () => {
    const due = await createOutbox("PENDING");
    const exhausted = await createOutbox("EXHAUSTED");
    const scheduler = new DeliveryScheduler({
      database,
      webhookDeliveryQueue: deliveryQueue,
      deadLetterQueue,
      logger,
      pollIntervalMs: 10,
      batchSize: 10,
      claimTimeoutMs: 1_000,
      maxAttempts: 5,
      now: () => new Date(NOW),
    });

    const result = await scheduler.publishOnce();

    expect(result).toEqual({
      deliveryClaimed: 1,
      deliveryPublished: 1,
      deliveryPublicationFailed: 0,
      staleClaimsRecovered: 0,
      abandonedAttempts: 0,
      staleAttemptsExhausted: 0,
      deadLetterClaimed: 1,
      deadLetterPublished: 1,
      deadLetterPublicationFailed: 0,
    });

    const deliveryJobs = await deliveryQueue.getJobs(["waiting"]);

    expect(deliveryJobs).toHaveLength(1);
    expect(deliveryJobs[0]).toMatchObject({
      name: JOB_NAMES.deliverWebhook,
      data: {
        outboxEventId: due.outboxEventId,
        organizationId: due.organizationId,
        correlationId: due.correlationId,
        expectedAttemptNumber: 1,
        claimStartedAt: NOW.toISOString(),
      },
      opts: {
        attempts: 1,
      },
    });

    const deadLetterJobs = await deadLetterQueue.getJobs(["waiting"]);

    expect(deadLetterJobs).toHaveLength(1);
    expect(deadLetterJobs[0]).toMatchObject({
      name: JOB_NAMES.deadLetteredJob,
      data: {
        sourceQueue: "webhook-delivery",
        sourceJobName: JOB_NAMES.deliverWebhook,
        outboxEventId: exhausted.outboxEventId,
        organizationId: exhausted.organizationId,
        serviceRequestId: exhausted.serviceRequestId,
        correlationId: exhausted.correlationId,
        attemptsMade: 5,
        failureReason: "Receiver returned HTTP 500",
        failedAt: DUE_AT.toISOString(),
      },
      opts: {
        attempts: 1,
      },
    });

    const [claimedOutbox, deadLetteredOutbox] = await Promise.all([
      database.outboxEvent.findUniqueOrThrow({
        where: {
          id: due.outboxEventId,
        },
      }),
      database.outboxEvent.findUniqueOrThrow({
        where: {
          id: exhausted.outboxEventId,
        },
      }),
    ]);

    expect(claimedOutbox).toMatchObject({
      status: "PROCESSING",
      attemptCount: 0,
      processingStartedAt: NOW,
      processedAt: null,
    });
    expect(deadLetteredOutbox).toMatchObject({
      status: "EXHAUSTED",
      attemptCount: 5,
      processingStartedAt: null,
      processedAt: NOW,
    });
  });

  it("terminally exhausts malformed work without an HTTP attempt and publishes one DLQ job", async () => {
    const malformed = await createOutbox("PENDING", true);
    const scheduler = new DeliveryScheduler({
      database,
      webhookDeliveryQueue: deliveryQueue,
      deadLetterQueue,
      logger,
      pollIntervalMs: 10,
      batchSize: 10,
      claimTimeoutMs: 1_000,
      maxAttempts: 5,
      now: () => new Date(NOW),
    });

    const firstCycle = await scheduler.publishOnce();
    const duplicateCycle = await scheduler.publishOnce();
    const [deliveryJobs, deadLetterJobs, outbox, deliveryHistoryCount] =
      await Promise.all([
        deliveryQueue.getJobs(["waiting"]),
        deadLetterQueue.getJobs(["waiting"]),
        database.outboxEvent.findUniqueOrThrow({
          where: {
            id: malformed.outboxEventId,
          },
        }),
        database.webhookDelivery.count({
          where: {
            outboxEventId: malformed.outboxEventId,
          },
        }),
      ]);

    expect(firstCycle).toEqual({
      deliveryClaimed: 1,
      deliveryPublished: 0,
      deliveryPublicationFailed: 1,
      staleClaimsRecovered: 0,
      abandonedAttempts: 0,
      staleAttemptsExhausted: 0,
      deadLetterClaimed: 1,
      deadLetterPublished: 1,
      deadLetterPublicationFailed: 0,
    });
    expect(duplicateCycle).toEqual({
      deliveryClaimed: 0,
      deliveryPublished: 0,
      deliveryPublicationFailed: 0,
      staleClaimsRecovered: 0,
      abandonedAttempts: 0,
      staleAttemptsExhausted: 0,
      deadLetterClaimed: 0,
      deadLetterPublished: 0,
      deadLetterPublicationFailed: 0,
    });
    expect(deliveryJobs).toEqual([]);
    expect(deadLetterJobs).toHaveLength(1);
    expect(deadLetterJobs[0]).toMatchObject({
      data: {
        sourceQueue: "webhook-delivery",
        outboxEventId: malformed.outboxEventId,
        organizationId: malformed.organizationId,
        serviceRequestId: malformed.serviceRequestId,
        correlationId: null,
        attemptsMade: 0,
        failureReason:
          "Assigned OutboxEvent payload is invalid or does not match its durable identity",
      },
    });
    expect(outbox).toMatchObject({
      status: "EXHAUSTED",
      attemptCount: 0,
      processingStartedAt: null,
      processedAt: NOW,
      lastError: {
        code: "INVALID_ASSIGNED_OUTBOX_PAYLOAD",
        failedAt: NOW.toISOString(),
      },
    });
    expect(deliveryHistoryCount).toBe(0);
  });

  it("starts and stops without waiting for the poll interval", async () => {
    const scheduler = new DeliveryScheduler({
      database,
      webhookDeliveryQueue: deliveryQueue,
      deadLetterQueue,
      logger,
      pollIntervalMs: 60_000,
      batchSize: 1,
      claimTimeoutMs: 1_000,
      maxAttempts: 5,
      now: () => new Date(NOW),
    });

    scheduler.start();

    expect(scheduler.isRunning).toBe(true);

    await scheduler.stop();

    expect(scheduler.isRunning).toBe(false);
  });
});
