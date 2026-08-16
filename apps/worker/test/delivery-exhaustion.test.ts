import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";

import { createDatabaseClient } from "@pulseroute/db";
import {
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
import { DeliveryScheduler } from "../src/delivery-scheduler.js";
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
  throw new Error("DATABASE_URL is required for delivery exhaustion tests");
}

if (!redisUrl) {
  throw new Error("REDIS_URL is required for delivery exhaustion tests");
}

const SECRET = "delivery-exhaustion-secret-is-at-least-32-characters";
const FIRST_ATTEMPT_AT = new Date("2005-01-01T00:01:00.000Z");
const SECOND_ATTEMPT_AT = new Date("2005-01-01T00:02:00.000Z");
const DUE_AT = new Date("2005-01-01T00:00:00.000Z");
const database = createDatabaseClient(databaseUrl);
const deliveryQueue = new Queue<WebhookDeliveryJobData>(
  `phase9-exhaustion-delivery-${randomUUID()}`,
  {
    connection: createProducerRedisOptions(redisUrl),
    skipWaitingForReady: true,
    defaultJobOptions: {
      attempts: 1,
      removeOnComplete: false,
      removeOnFail: false,
    },
  },
);
const deliveryQueueEvents = new QueueEvents(deliveryQueue.name, {
  connection: createWorkerRedisOptions(redisUrl),
});
const deadLetterQueue = new Queue<DeadLetteredJobData>(
  `phase9-exhaustion-dlq-${randomUUID()}`,
  {
    connection: createProducerRedisOptions(redisUrl),
    skipWaitingForReady: true,
  },
);
const logger = createWorkerLogger({
  nodeEnv: "test",
  logLevel: "silent",
});

let deliveryWorker:
  | Worker<WebhookDeliveryJobData, WebhookDeliveryProcessorResult, string>
  | undefined;
let receiver: RunningFakeReceiver | undefined;
let organizationId: string | undefined;

async function waitForAttemptJob(attemptNumber: number) {
  const deadline = Date.now() + 5_000;

  while (Date.now() < deadline) {
    const jobs = await deliveryQueue.getJobs([
      "waiting",
      "active",
      "completed",
      "failed",
    ]);
    const job = jobs.find(
      (candidate) => candidate.data.expectedAttemptNumber === attemptNumber,
    );

    if (job) {
      return job;
    }

    await new Promise((resolve) => {
      setTimeout(resolve, 10);
    });
  }

  throw new Error(`Timed out waiting for delivery attempt ${attemptNumber}`);
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
  await deliveryWorker?.close();
  await deliveryQueueEvents.close();
  await Promise.all([
    deliveryQueue.obliterate({ force: true }).catch(() => undefined),
    deadLetterQueue.obliterate({ force: true }).catch(() => undefined),
  ]);
  await Promise.all([deliveryQueue.close(), deadLetterQueue.close()]);
  await receiver?.close();
  await clearFixture();
  await database.$disconnect();
});

describe("webhook delivery exhaustion", () => {
  it("records each 500 attempt and publishes exactly one deterministic DLQ job", async () => {
    receiver = await startFakeReceiver({
      secret: SECRET,
      mode: "failure",
    });
    await Promise.all([
      deliveryQueue.waitUntilReady(),
      deliveryQueueEvents.waitUntilReady(),
      deadLetterQueue.waitUntilReady(),
    ]);
    await Promise.all([
      deliveryQueue.obliterate({ force: true }),
      deadLetterQueue.obliterate({ force: true }),
    ]);

    let clock = FIRST_ATTEMPT_AT;

    deliveryWorker = new Worker<
      WebhookDeliveryJobData,
      WebhookDeliveryProcessorResult,
      string
    >(
      deliveryQueue.name,
      createWebhookDeliveryProcessor({
        database,
        logger,
        webhookUrl: receiver.url,
        webhookSecret: SECRET,
        timeoutMs: 1_000,
        maxAttempts: 2,
        baseDelayMs: 1,
        maxDelayMs: 1,
        now: () => new Date(clock),
        random: () => 0,
      }),
      {
        connection: createWorkerRedisOptions(redisUrl),
        concurrency: 1,
      },
    );

    await deliveryWorker.waitUntilReady();

    organizationId = randomUUID();
    const outboxEventId = randomUUID();
    const serviceRequestId = randomUUID();
    const correlationId = `delivery-exhaustion-${randomUUID()}`;

    await database.organization.create({
      data: {
        id: organizationId,
        name: "Delivery Exhaustion Test",
      },
    });
    await database.outboxEvent.create({
      data: {
        id: outboxEventId,
        organizationId,
        eventType: "service_request.assigned",
        aggregateType: "service_request",
        aggregateId: serviceRequestId,
        nextAttemptAt: DUE_AT,
        createdAt: DUE_AT,
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

    const scheduler = new DeliveryScheduler({
      database,
      webhookDeliveryQueue: deliveryQueue,
      deadLetterQueue,
      logger,
      pollIntervalMs: 10,
      batchSize: 1,
      claimTimeoutMs: 5_000,
      maxAttempts: 2,
      now: () => new Date(clock),
    });

    await scheduler.publishOnce();

    const firstJob = await waitForAttemptJob(1);
    const firstResult = await firstJob.waitUntilFinished(
      deliveryQueueEvents,
      5_000,
    );

    expect(firstResult).toMatchObject({
      kind: "retry_scheduled",
      attemptNumber: 1,
      httpStatus: 500,
    });

    clock = SECOND_ATTEMPT_AT;

    await scheduler.publishOnce();

    const secondJob = await waitForAttemptJob(2);
    const secondResult = await secondJob.waitUntilFinished(
      deliveryQueueEvents,
      5_000,
    );

    expect(secondResult).toMatchObject({
      kind: "exhausted",
      attemptNumber: 2,
      httpStatus: 500,
    });

    const dlqCycle = await scheduler.publishOnce();
    const duplicateDlqCycle = await scheduler.publishOnce();
    const deadLetterJobs = await deadLetterQueue.getJobs(["waiting"]);

    expect(dlqCycle.deadLetterPublished).toBe(1);
    expect(duplicateDlqCycle.deadLetterPublished).toBe(0);
    expect(deadLetterJobs).toHaveLength(1);
    expect(deadLetterJobs[0]).toMatchObject({
      data: {
        sourceQueue: "webhook-delivery",
        outboxEventId,
        organizationId,
        serviceRequestId,
        correlationId,
        attemptsMade: 2,
      },
    });

    const [outbox, attempts] = await Promise.all([
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

    expect(outbox).toMatchObject({
      status: "EXHAUSTED",
      attemptCount: 2,
      processingStartedAt: null,
      processedAt: SECOND_ATTEMPT_AT,
    });
    expect(attempts).toHaveLength(2);
    expect(attempts.map((attempt) => attempt.status)).toEqual([
      "FAILED",
      "FAILED",
    ]);
    expect(receiver.getRequests()).toHaveLength(2);
  });
});
