import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";

import { createDatabaseClient } from "@pulseroute/db";
import { JOB_NAMES, type WebhookDeliveryJobData } from "@pulseroute/shared";
import { Queue, QueueEvents, Worker } from "bullmq";
import { config as loadEnvironmentFile } from "dotenv";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";

import {
  startFakeReceiver,
  type RunningFakeReceiver,
} from "@pulseroute/fake-receiver";
import { claimDueWebhookDeliveries } from "../src/delivery-claimer.js";
import { createWorkerLogger } from "../src/logger.js";
import {
  createProducerRedisOptions,
  createWorkerRedisOptions,
} from "../src/redis.js";
import {
  calculateFullJitterDelay,
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
  throw new Error("DATABASE_URL is required for webhook delivery worker tests");
}

if (!redisUrl) {
  throw new Error("REDIS_URL is required for webhook delivery worker tests");
}
const configuredRedisUrl = redisUrl;

const SECRET = "webhook-delivery-worker-secret-is-at-least-32-characters";
const DUE_AT = new Date("2001-01-01T00:00:00.000Z");
const FIRST_CLAIM_AT = new Date("2001-01-01T00:01:00.000Z");
const SECOND_CLAIM_AT = new Date("2001-01-01T00:02:00.000Z");
const THIRD_CLAIM_AT = new Date("2001-01-01T00:03:00.000Z");
const STALE_BEFORE = new Date("2001-01-01T00:00:30.000Z");

const database = createDatabaseClient(databaseUrl);
const queueName = `phase9-delivery-worker-${randomUUID()}`;
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

const organizationIds: string[] = [];
let receiver: RunningFakeReceiver | undefined;
let worker:
  | Worker<WebhookDeliveryJobData, WebhookDeliveryProcessorResult, string>
  | undefined;
let processorNow = FIRST_CLAIM_AT;

async function createAssignedOutboxFixture() {
  const organizationId = randomUUID();
  const outboxEventId = randomUUID();
  const serviceRequestId = randomUUID();
  const correlationId = `delivery-worker-${randomUUID()}`;

  organizationIds.push(organizationId);

  await database.organization.create({
    data: {
      id: organizationId,
      name: `Delivery Worker Test ${organizationId}`,
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

  return {
    organizationId,
    outboxEventId,
    serviceRequestId,
    correlationId,
  };
}

async function claimOne(claimStartedAt: Date) {
  const result = await claimDueWebhookDeliveries({
    database,
    batchSize: 1,
    claimStartedAt,
    staleBefore: STALE_BEFORE,
    maxAttempts: 3,
  });

  expect(result.claimed).toHaveLength(1);

  const claim = result.claimed[0]!;
  const payload = claim.payload as {
    correlationId: string;
  };

  return {
    outboxEventId: claim.outboxEventId,
    organizationId: claim.organizationId,
    correlationId: payload.correlationId,
    expectedAttemptNumber: claim.expectedAttemptNumber,
    claimStartedAt: claim.claimStartedAt,
  } satisfies WebhookDeliveryJobData;
}

async function clearDatabaseFixtures(): Promise<void> {
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

function createTestDeliveryWorker(): Worker<
  WebhookDeliveryJobData,
  WebhookDeliveryProcessorResult,
  string
> {
  if (!receiver) {
    throw new Error("Fake receiver was not initialized");
  }

  return new Worker<
    WebhookDeliveryJobData,
    WebhookDeliveryProcessorResult,
    string
  >(
    queueName,
    createWebhookDeliveryProcessor({
      database,
      logger,
      webhookUrl: receiver.url,
      webhookSecret: SECRET,
      timeoutMs: 500,
      maxAttempts: 3,
      baseDelayMs: 100,
      maxDelayMs: 1_000,
      now: () => new Date(processorNow),
      random: () => 0.5,
    }),
    {
      connection: createWorkerRedisOptions(configuredRedisUrl),
      concurrency: 2,
    },
  );
}

async function waitForReceiverRequest(requestIndex: number, timeoutMs = 2_000) {
  const deadline = Date.now() + timeoutMs;

  while (Date.now() < deadline) {
    const request = receiver?.getRequests()[requestIndex];

    if (request) {
      return request;
    }

    await new Promise<void>((resolve) => {
      setTimeout(resolve, 5);
    });
  }

  throw new Error("Timed out waiting for the active fake receiver request");
}

function createDrainTimeout(milliseconds: number): Promise<never> {
  return new Promise((_, reject) => {
    setTimeout(() => {
      reject(new Error("Webhook delivery worker drain timed out"));
    }, milliseconds).unref();
  });
}

beforeAll(async () => {
  receiver = await startFakeReceiver({
    secret: SECRET,
    mode: "success",
    delayMs: 1_000,
  });

  await Promise.all([queue.waitUntilReady(), queueEvents.waitUntilReady()]);
  await queue.obliterate({ force: true });

  worker = createTestDeliveryWorker();
  await worker.waitUntilReady();
});

afterEach(async () => {
  await queue.obliterate({ force: true });
  await clearDatabaseFixtures();
  receiver?.setMode("success");
});

afterAll(async () => {
  await worker?.close();
  await queueEvents.close();
  await queue.obliterate({ force: true }).catch(() => undefined);
  await queue.close();
  await receiver?.close();
  await clearDatabaseFixtures();
  await database.$disconnect();
});

describe("webhook delivery worker", () => {
  it("preserves two failed attempts before a later PostgreSQL retry succeeds", async () => {
    if (!receiver) {
      throw new Error("Fake receiver was not initialized");
    }

    const fixture = await createAssignedOutboxFixture();

    receiver.setModeSequence(["failure", "failure", "success"]);
    processorNow = FIRST_CLAIM_AT;

    const firstJobData = await claimOne(FIRST_CLAIM_AT);
    const firstJob = await queue.add(JOB_NAMES.deliverWebhook, firstJobData, {
      jobId: `delivery-test-${fixture.outboxEventId}-1`,
    });
    const firstResult = await firstJob.waitUntilFinished(queueEvents, 5_000);

    expect(firstResult).toMatchObject({
      kind: "retry_scheduled",
      outboxEventId: fixture.outboxEventId,
      attemptNumber: 1,
      httpStatus: 500,
      delayMs: 50,
      nextAttemptAt: "2001-01-01T00:01:00.050Z",
    });

    const afterFailure = await database.outboxEvent.findUniqueOrThrow({
      where: {
        id: fixture.outboxEventId,
      },
    });

    expect(afterFailure).toMatchObject({
      status: "PENDING",
      attemptCount: 1,
      processingStartedAt: null,
      processedAt: null,
      nextAttemptAt: new Date("2001-01-01T00:01:00.050Z"),
    });

    await database.outboxEvent.update({
      where: {
        id: fixture.outboxEventId,
      },
      data: {
        nextAttemptAt: SECOND_CLAIM_AT,
      },
    });

    processorNow = SECOND_CLAIM_AT;

    const secondJobData = await claimOne(SECOND_CLAIM_AT);
    const secondJob = await queue.add(JOB_NAMES.deliverWebhook, secondJobData, {
      jobId: `delivery-test-${fixture.outboxEventId}-2`,
    });
    const secondResult = await secondJob.waitUntilFinished(queueEvents, 5_000);

    expect(secondResult).toMatchObject({
      kind: "retry_scheduled",
      outboxEventId: fixture.outboxEventId,
      attemptNumber: 2,
      httpStatus: 500,
      delayMs: 100,
      nextAttemptAt: "2001-01-01T00:02:00.100Z",
    });

    await database.outboxEvent.update({
      where: {
        id: fixture.outboxEventId,
      },
      data: {
        nextAttemptAt: THIRD_CLAIM_AT,
      },
    });

    processorNow = THIRD_CLAIM_AT;

    const thirdJobData = await claimOne(THIRD_CLAIM_AT);
    const thirdJob = await queue.add(JOB_NAMES.deliverWebhook, thirdJobData, {
      jobId: `delivery-test-${fixture.outboxEventId}-3`,
    });
    const thirdResult = await thirdJob.waitUntilFinished(queueEvents, 5_000);

    expect(thirdResult).toMatchObject({
      kind: "delivered",
      outboxEventId: fixture.outboxEventId,
      attemptNumber: 3,
      httpStatus: 200,
    });

    const [deliveredOutbox, history] = await Promise.all([
      database.outboxEvent.findUniqueOrThrow({
        where: {
          id: fixture.outboxEventId,
        },
      }),
      database.webhookDelivery.findMany({
        where: {
          outboxEventId: fixture.outboxEventId,
        },
        orderBy: {
          attemptNumber: "asc",
        },
      }),
    ]);

    expect(deliveredOutbox).toMatchObject({
      status: "DELIVERED",
      attemptCount: 3,
      processingStartedAt: null,
      processedAt: THIRD_CLAIM_AT,
      lastError: null,
    });
    expect(history).toHaveLength(3);
    expect(
      history.map((attempt) => [attempt.attemptNumber, attempt.status]),
    ).toEqual([
      [1, "FAILED"],
      [2, "FAILED"],
      [3, "SUCCEEDED"],
    ]);
    expect(receiver.getRequests()).toHaveLength(3);
    expect(
      receiver.getRequests().every((request) => request.signatureAccepted),
    ).toBe(true);
  });

  it("drains a real active timeout request within a bounded worker.close", async () => {
    if (!receiver || !worker) {
      throw new Error("Delivery worker test resources were not initialized");
    }

    const requestsBefore = receiver.getRequests().length;
    const fixture = await createAssignedOutboxFixture();

    receiver.setMode("timeout");
    processorNow = FIRST_CLAIM_AT;

    const jobData = await claimOne(FIRST_CLAIM_AT);
    const job = await queue.add(JOB_NAMES.deliverWebhook, jobData, {
      jobId: `delivery-timeout-${fixture.outboxEventId}`,
    });
    const completionPromise = job.waitUntilFinished(queueEvents, 5_000);
    const drainingWorker = worker;
    let activeRequest:
      Awaited<ReturnType<typeof waitForReceiverRequest>> | undefined;
    let result: WebhookDeliveryProcessorResult | undefined;
    let drainDurationMs: number | undefined;
    let closeStarted = false;

    try {
      activeRequest = await waitForReceiverRequest(requestsBefore);
      const closeStartedAt = Date.now();

      closeStarted = true;
      worker = undefined;

      const [, completedResult] = await Promise.all([
        Promise.race([drainingWorker.close(), createDrainTimeout(2_000)]),
        completionPromise,
      ]);

      drainDurationMs = Date.now() - closeStartedAt;
      result = completedResult;
    } finally {
      if (!closeStarted) {
        await drainingWorker.close().catch(() => undefined);
      }

      worker = createTestDeliveryWorker();
      await worker.waitUntilReady();
    }

    expect(activeRequest).toMatchObject({
      signatureAccepted: true,
      mode: "timeout",
      statusCode: null,
      outcome: "pending",
    });
    expect(drainDurationMs).toBeLessThan(2_000);
    expect(result).toMatchObject({
      kind: "retry_scheduled",
      attemptNumber: 1,
      httpStatus: null,
    });

    const delivery = await database.webhookDelivery.findFirstOrThrow({
      where: {
        outboxEventId: fixture.outboxEventId,
      },
    });

    expect(delivery).toMatchObject({
      status: "FAILED",
      httpStatus: null,
      errorDetails: {
        code: "TIMEOUT",
        outcome: "timeout",
      },
    });
    expect(
      await database.webhookDelivery.count({
        where: {
          outboxEventId: fixture.outboxEventId,
          status: "STARTED",
        },
      }),
    ).toBe(0);
  });

  it("allows only one duplicate job to start the durable attempt", async () => {
    if (!receiver) {
      throw new Error("Fake receiver was not initialized");
    }

    const requestsBefore = receiver.getRequests().length;

    const fixture = await createAssignedOutboxFixture();

    receiver.setMode("success");
    processorNow = FIRST_CLAIM_AT;

    const jobData = await claimOne(FIRST_CLAIM_AT);
    const [firstJob, duplicateJob] = await Promise.all([
      queue.add(JOB_NAMES.deliverWebhook, jobData, {
        jobId: `delivery-duplicate-a-${fixture.outboxEventId}`,
      }),
      queue.add(JOB_NAMES.deliverWebhook, jobData, {
        jobId: `delivery-duplicate-b-${fixture.outboxEventId}`,
      }),
    ]);
    const results = await Promise.all([
      firstJob.waitUntilFinished(queueEvents, 5_000),
      duplicateJob.waitUntilFinished(queueEvents, 5_000),
    ]);

    expect(results.map((result) => result.kind).sort()).toEqual([
      "delivered",
      "stale_job",
    ]);
    expect(
      await database.webhookDelivery.count({
        where: {
          outboxEventId: fixture.outboxEventId,
        },
      }),
    ).toBe(1);
    expect(receiver.getRequests()).toHaveLength(requestsBefore + 1);
  });

  it("terminally exhausts payload corruption after claim without recording an HTTP attempt", async () => {
    if (!receiver) {
      throw new Error("Fake receiver was not initialized");
    }

    const requestsBefore = receiver.getRequests().length;
    const fixture = await createAssignedOutboxFixture();

    processorNow = FIRST_CLAIM_AT;

    const jobData = await claimOne(FIRST_CLAIM_AT);

    await database.outboxEvent.update({
      where: {
        id: fixture.outboxEventId,
      },
      data: {
        payload: {
          organizationId: fixture.organizationId,
          serviceRequestId: fixture.serviceRequestId,
          correlationId: fixture.correlationId,
        },
      },
    });

    const job = await queue.add(JOB_NAMES.deliverWebhook, jobData, {
      jobId: `delivery-malformed-${fixture.outboxEventId}`,
    });
    const result = await job.waitUntilFinished(queueEvents, 5_000);
    const [outbox, deliveryHistoryCount] = await Promise.all([
      database.outboxEvent.findUniqueOrThrow({
        where: {
          id: fixture.outboxEventId,
        },
      }),
      database.webhookDelivery.count({
        where: {
          outboxEventId: fixture.outboxEventId,
        },
      }),
    ]);

    expect(result).toEqual({
      kind: "invalid_outbox_event",
      outboxEventId: fixture.outboxEventId,
      attemptCount: 0,
    });
    expect(outbox).toMatchObject({
      status: "EXHAUSTED",
      attemptCount: 0,
      processingStartedAt: null,
      processedAt: null,
      lastError: {
        code: "INVALID_ASSIGNED_OUTBOX_PAYLOAD",
      },
    });
    expect(deliveryHistoryCount).toBe(0);
    expect(receiver.getRequests()).toHaveLength(requestsBefore);
  });
});

describe("full jitter", () => {
  it.each([
    [0, 0, 1_000, 0],
    [0, 0.5, 1_000, 500],
    [1, 0.5, 2_000, 1_000],
    [2, 0.999, 4_000, 3_996],
    [20, 0.5, 60_000, 30_000],
  ])(
    "calculates retry index %i with random %f inside cap %i",
    (retryIndex, randomValue, expectedCap, expectedDelay) => {
      expect(
        calculateFullJitterDelay({
          retryIndex,
          baseDelayMs: 1_000,
          maxDelayMs: 60_000,
          randomValue,
        }),
      ).toEqual({
        exponentialCapMs: expectedCap,
        delayMs: expectedDelay,
      });
    },
  );
});
