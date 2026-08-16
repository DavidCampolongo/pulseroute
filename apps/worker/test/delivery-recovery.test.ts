import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";

import { createDatabaseClient } from "@pulseroute/db";
import {
  JOB_NAMES,
  QUEUE_NAMES,
  type DeadLetteredJobData,
  type WebhookDeliveryJobData,
} from "@pulseroute/shared";
import { Queue, QueueEvents, Worker } from "bullmq";
import { config as loadEnvironmentFile } from "dotenv";
import { afterAll, describe, expect, it } from "vitest";

import {
  startFakeReceiver,
  type RunningFakeReceiver,
} from "@pulseroute/fake-receiver";
import { claimDueWebhookDeliveries } from "../src/delivery-claimer.js";
import { recoverDeadLetteredWebhookDelivery } from "../src/delivery-recovery.js";
import { createWorkerLogger } from "../src/logger.js";
import {
  createProducerRedisOptions,
  createWorkerRedisOptions,
} from "../src/redis.js";
import {
  createWebhookDeliveryProcessor,
  type WebhookDeliveryProcessorResult,
} from "../src/webhook-delivery-worker.js";

loadEnvironmentFile({
  path: fileURLToPath(new URL("../../../.env", import.meta.url)),
  quiet: true,
});

const databaseUrl = process.env.DATABASE_URL;
const redisUrl = process.env.REDIS_URL;

if (!databaseUrl) {
  throw new Error("DATABASE_URL is required for delivery recovery tests");
}

if (!redisUrl) {
  throw new Error("REDIS_URL is required for delivery recovery tests");
}

const SECRET = "delivery-recovery-secret-is-at-least-32-characters";
const FIRST_ATTEMPT_AT = new Date("2003-01-01T00:01:00.000Z");
const RECOVERED_AT = new Date("2003-01-01T00:02:00.000Z");
const STALE_BEFORE = new Date("2003-01-01T00:00:00.000Z");
const database = createDatabaseClient(databaseUrl);
const queueName = `phase9-delivery-recovery-${randomUUID()}`;
const queue = new Queue<WebhookDeliveryJobData>(queueName, {
  connection: createProducerRedisOptions(redisUrl),
  skipWaitingForReady: true,
  defaultJobOptions: {
    attempts: 1,
    removeOnComplete: false,
    removeOnFail: false,
  },
});
const queueEvents = new QueueEvents(queueName, {
  connection: createWorkerRedisOptions(redisUrl),
});
const logger = createWorkerLogger({
  nodeEnv: "test",
  logLevel: "silent",
});

let worker:
  | Worker<WebhookDeliveryJobData, WebhookDeliveryProcessorResult, string>
  | undefined;
let receiver: RunningFakeReceiver | undefined;
let organizationId: string | undefined;

async function claimJobData(claimStartedAt: Date) {
  const claim = await claimDueWebhookDeliveries({
    database,
    batchSize: 1,
    claimStartedAt,
    staleBefore: STALE_BEFORE,
    maxAttempts: 1,
  });

  expect(claim.claimed).toHaveLength(1);

  const claimed = claim.claimed[0]!;

  return {
    outboxEventId: claimed.outboxEventId,
    organizationId: claimed.organizationId,
    correlationId: (claimed.payload as { correlationId: string }).correlationId,
    expectedAttemptNumber: claimed.expectedAttemptNumber,
    claimStartedAt: claimed.claimStartedAt,
  } satisfies WebhookDeliveryJobData;
}

async function clearFixture(): Promise<void> {
  if (!organizationId) {
    return;
  }

  await database.webhookDelivery.deleteMany({
    where: {
      organizationId,
    },
  });
  await database.outboxEvent.deleteMany({
    where: {
      organizationId,
    },
  });
  await database.organization.deleteMany({
    where: {
      id: organizationId,
    },
  });
  organizationId = undefined;
}

afterAll(async () => {
  await worker?.close();
  await queueEvents.close();
  await queue.obliterate({ force: true }).catch(() => undefined);
  await queue.close();
  await receiver?.close();
  await clearFixture();
  await database.$disconnect();
});

describe("dead-lettered webhook recovery", () => {
  it("survives a stopped receiver, preserves failure history, and succeeds after restart", async () => {
    const portProbe = await startFakeReceiver({
      secret: SECRET,
      mode: "success",
    });
    const stoppedReceiverPort = portProbe.port;

    await portProbe.close();

    const deliveryUrl = `http://127.0.0.1:${stoppedReceiverPort}/webhooks`;
    let processorNow = FIRST_ATTEMPT_AT;

    await Promise.all([queue.waitUntilReady(), queueEvents.waitUntilReady()]);
    await queue.obliterate({ force: true });

    worker = new Worker<
      WebhookDeliveryJobData,
      WebhookDeliveryProcessorResult,
      string
    >(
      queueName,
      createWebhookDeliveryProcessor({
        database,
        logger,
        webhookUrl: deliveryUrl,
        webhookSecret: SECRET,
        timeoutMs: 250,
        maxAttempts: 1,
        baseDelayMs: 10,
        maxDelayMs: 10,
        now: () => new Date(processorNow),
        random: () => 0,
      }),
      {
        connection: createWorkerRedisOptions(redisUrl),
        concurrency: 1,
      },
    );

    await worker.waitUntilReady();

    organizationId = randomUUID();
    const outboxEventId = randomUUID();
    const serviceRequestId = randomUUID();
    const correlationId = `delivery-recovery-${randomUUID()}`;

    await database.organization.create({
      data: {
        id: organizationId,
        name: "Delivery Recovery Test",
      },
    });
    await database.outboxEvent.create({
      data: {
        id: outboxEventId,
        organizationId,
        eventType: "service_request.assigned",
        aggregateType: "service_request",
        aggregateId: serviceRequestId,
        status: "PENDING",
        nextAttemptAt: STALE_BEFORE,
        createdAt: STALE_BEFORE,
        payload: {
          organizationId,
          serviceRequestId,
          operatorId: randomUUID(),
          assignmentId: randomUUID(),
          routingDecisionId: randomUUID(),
          scoringVersion: "pulseroute-scoring-v1",
          correlationId,
        },
      },
    });

    const firstJob = await queue.add(
      JOB_NAMES.deliverWebhook,
      await claimJobData(FIRST_ATTEMPT_AT),
      {
        jobId: `recovery-network-failure-${outboxEventId}`,
      },
    );
    const failedResult = await firstJob.waitUntilFinished(queueEvents, 5_000);

    expect(failedResult).toMatchObject({
      kind: "exhausted",
      outboxEventId,
      attemptNumber: 1,
      httpStatus: null,
    });

    const exhausted = await database.outboxEvent.findUniqueOrThrow({
      where: {
        id: outboxEventId,
      },
    });

    expect(exhausted).toMatchObject({
      status: "EXHAUSTED",
      attemptCount: 1,
      processingStartedAt: null,
      processedAt: null,
      lastError: {
        code: "NETWORK_ERROR",
        outcome: "network_failure",
      },
    });

    const deadLetter: DeadLetteredJobData = {
      sourceQueue: QUEUE_NAMES.webhookDelivery,
      sourceJobId: `webhook-delivery-${outboxEventId}-1`,
      sourceJobName: JOB_NAMES.deliverWebhook,
      outboxEventId,
      organizationId,
      serviceRequestId,
      correlationId,
      attemptsMade: 1,
      failureReason: "Receiver unavailable",
      failedAt: FIRST_ATTEMPT_AT.toISOString(),
    };
    const recovery = await recoverDeadLetteredWebhookDelivery({
      database,
      deadLetter,
      recoveredAt: RECOVERED_AT,
    });

    expect(recovery).toEqual({
      kind: "recovered",
      outboxEventId,
      attemptCount: 1,
      nextExpectedAttemptNumber: 2,
      nextAttemptAt: RECOVERED_AT.toISOString(),
    });

    receiver = await startFakeReceiver({
      secret: SECRET,
      port: stoppedReceiverPort,
      mode: "success",
    });
    processorNow = RECOVERED_AT;

    const recoveryJob = await queue.add(
      JOB_NAMES.deliverWebhook,
      await claimJobData(RECOVERED_AT),
      {
        jobId: `recovery-success-${outboxEventId}`,
      },
    );
    const recoveryResult = await recoveryJob.waitUntilFinished(
      queueEvents,
      5_000,
    );

    expect(recoveryResult).toMatchObject({
      kind: "delivered",
      outboxEventId,
      attemptNumber: 2,
      httpStatus: 200,
    });

    const [delivered, history] = await Promise.all([
      database.outboxEvent.findUniqueOrThrow({
        where: {
          id: outboxEventId,
        },
      }),
      database.webhookDelivery.findMany({
        where: {
          outboxEventId,
        },
        orderBy: {
          attemptNumber: "asc",
        },
      }),
    ]);

    expect(delivered).toMatchObject({
      status: "DELIVERED",
      attemptCount: 2,
      lastError: null,
      processedAt: RECOVERED_AT,
    });
    expect(
      history.map((attempt) => [attempt.attemptNumber, attempt.status]),
    ).toEqual([
      [1, "FAILED"],
      [2, "SUCCEEDED"],
    ]);
    expect(receiver.getRequests()).toHaveLength(1);
    expect(receiver.getRequests()[0]).toMatchObject({
      signatureAccepted: true,
      statusCode: 200,
    });

    const laterExhaustion = await database.outboxEvent.update({
      where: {
        id: outboxEventId,
      },
      data: {
        status: "EXHAUSTED",
        processedAt: RECOVERED_AT,
        lastError: {
          code: "HTTP_ERROR",
          message: "A later recovery generation exhausted",
          failedAt: RECOVERED_AT.toISOString(),
        },
      },
    });
    const staleReplay = await recoverDeadLetteredWebhookDelivery({
      database,
      deadLetter,
      recoveredAt: new Date("2003-01-01T00:03:00.000Z"),
    });
    const afterStaleReplay = await database.outboxEvent.findUniqueOrThrow({
      where: {
        id: outboxEventId,
      },
    });

    expect(staleReplay).toEqual({
      kind: "stale_recovery_generation",
      outboxEventId,
      deadLetterAttemptCount: 1,
      currentAttemptCount: 2,
    });
    expect(afterStaleReplay).toEqual(laterExhaustion);
  });
});
