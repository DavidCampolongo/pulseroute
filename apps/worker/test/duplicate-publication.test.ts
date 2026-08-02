import { fileURLToPath } from "node:url";

import { createDatabaseClient } from "@pulseroute/db";
import {
  EVENT_TYPES,
  JOB_NAMES,
  QUEUE_NAMES,
  type RouteServiceRequestJobData,
  type ServiceRequestIngestedJobData,
} from "@pulseroute/shared";
import { type Job, type Queue } from "bullmq";
import { config as loadEnvironmentFile } from "dotenv";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import {
  createIncomingWorker,
  createRoutingJobId,
} from "../src/incoming-worker.js";
import { createWorkerLogger } from "../src/logger.js";
import {
  createIncomingJobId,
  InternalOutboxPublisher,
} from "../src/outbox-publisher.js";
import {
  closePulseRouteQueues,
  createPulseRouteQueues,
  type PulseRouteQueues,
  waitForPulseRouteQueues,
} from "../src/queues.js";

loadEnvironmentFile({
  path: fileURLToPath(new URL("../../../.env", import.meta.url)),
  quiet: true,
});

const databaseUrl = process.env.DATABASE_URL;
const baseRedisUrl = process.env.REDIS_URL;

if (!databaseUrl) {
  throw new Error("DATABASE_URL is required for duplicate-publication tests");
}

if (!baseRedisUrl) {
  throw new Error("REDIS_URL is required for duplicate-publication tests");
}

/*
 * createIncomingWorker consumes the fixed production queue name.
 * Use a dedicated logical Redis database so this suite remains isolated.
 */
const testRedisUrl = new URL(baseRedisUrl);

testRedisUrl.pathname = "/12";

const redisUrl = testRedisUrl.toString();

const database = createDatabaseClient(databaseUrl);

const logger = createWorkerLogger({
  nodeEnv: "test",
  logLevel: "silent",
});

const organizationId = "60000001-0000-4000-8000-000000000012";

const requiredSkillId = "60000002-0000-4000-8000-000000000012";

const serviceRequestId = "60000003-0000-4000-8000-000000000012";

const outboxEventId = "60000004-0000-4000-8000-000000000012";

const correlationId = "phase6-duplicate-publication-correlation";

const externalRequestId = "phase6-duplicate-publication-request";

/*
 * The publisher scans all globally eligible events.
 *
 * A deliberately old clock isolates this fixture from ordinary application
 * events while the fixed organization ID lets interrupted prior runs clean up.
 */
const publicationNow = new Date("1900-01-01T00:00:00.000Z");

const eventCreatedAt = new Date("1899-12-31T23:58:00.000Z");

const eventDueAt = new Date("1899-12-31T23:59:00.000Z");

let queues: PulseRouteQueues | undefined;

let incomingWorker: ReturnType<typeof createIncomingWorker> | undefined;

function sleep(milliseconds: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, milliseconds);
  });
}

async function waitForJobState<DataType>(
  job: Job<DataType>,
  expectedState: string,
  timeoutMs = 5_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;

  while (Date.now() < deadline) {
    const state = await job.getState();

    if (state === expectedState) {
      return;
    }

    await sleep(25);
  }

  const finalState = await job.getState();

  throw new Error(
    `Timed out waiting for job ${job.id} to reach ${expectedState}; final state was ${finalState}`,
  );
}

async function waitForJob<DataType>(
  queue: Queue<DataType>,
  jobId: string,
  timeoutMs = 5_000,
): Promise<Job<DataType>> {
  const deadline = Date.now() + timeoutMs;

  while (Date.now() < deadline) {
    const job = await queue.getJob(jobId);

    if (job) {
      return job;
    }

    await sleep(25);
  }

  throw new Error(`Timed out waiting for BullMQ job ${jobId}`);
}

async function countQueueJobs<DataType>(
  queue: Queue<DataType>,
): Promise<number> {
  const counts = await queue.getJobCounts(
    "waiting",
    "active",
    "delayed",
    "completed",
    "failed",
  );

  return Object.values(counts).reduce((total, count) => total + count, 0);
}

async function obliterateQueues(activeQueues: PulseRouteQueues): Promise<void> {
  await Promise.all([
    activeQueues.incomingEvents
      .obliterate({
        force: true,
      })
      .catch(() => undefined),

    activeQueues.routing
      .obliterate({
        force: true,
      })
      .catch(() => undefined),

    activeQueues.notifications
      .obliterate({
        force: true,
      })
      .catch(() => undefined),

    activeQueues.webhookDelivery
      .obliterate({
        force: true,
      })
      .catch(() => undefined),

    activeQueues.deadLetter
      .obliterate({
        force: true,
      })
      .catch(() => undefined),
  ]);
}

async function clearDatabaseFixture(): Promise<void> {
  await database.outboxEvent.deleteMany({
    where: {
      organizationId,
    },
  });

  await database.serviceRequest.deleteMany({
    where: {
      organizationId,
    },
  });

  await database.skill.deleteMany({
    where: {
      organizationId,
    },
  });

  await database.organization.deleteMany({
    where: {
      id: organizationId,
    },
  });
}

beforeAll(async () => {
  await clearDatabaseFixture();

  await database.organization.create({
    data: {
      id: organizationId,
      name: "Phase 6 Duplicate Publication Test Organization",
    },
  });

  await database.skill.create({
    data: {
      id: requiredSkillId,
      organizationId,
      name: "Phase 6 Duplicate Publication Test Skill",
    },
  });

  await database.serviceRequest.create({
    data: {
      id: serviceRequestId,
      organizationId,
      externalId: externalRequestId,
      requiredSkillId,
      priority: "HIGH",
      region: "WEST",
    },
  });

  await database.outboxEvent.create({
    data: {
      id: outboxEventId,
      organizationId,
      aggregateType: "service_request",
      aggregateId: serviceRequestId,
      eventType: EVENT_TYPES.serviceRequestCreated,
      status: "PENDING",
      payload: {
        serviceRequestId,
        correlationId,
        externalId: externalRequestId,
        requiredSkillId,
        priority: "HIGH",
        region: "WEST",
      },
      createdAt: eventCreatedAt,
      nextAttemptAt: eventDueAt,
    },
  });

  queues = createPulseRouteQueues(redisUrl);

  await waitForPulseRouteQueues(queues);

  await obliterateQueues(queues);
});

afterAll(async () => {
  if (incomingWorker) {
    await incomingWorker.close();
  }

  if (queues) {
    await obliterateQueues(queues);

    await closePulseRouteQueues(queues);
  }

  await clearDatabaseFixture();

  await database.$disconnect();
});

describe("outbox duplicate publication tolerance", () => {
  it("converges a repeated publication on one incoming and one routing job", async () => {
    if (!queues) {
      throw new Error("Duplicate-publication queues were not initialized");
    }

    const incomingJobId = createIncomingJobId(outboxEventId);

    const incomingJobData: ServiceRequestIngestedJobData = {
      outboxEventId,
      organizationId,
      serviceRequestId,
      correlationId,
    };

    /*
     * Simulate the first half of the outbox dual-write failure window:
     *
     * queue.add() succeeded, but the process stopped before PostgreSQL was
     * updated from PENDING to DELIVERED.
     */
    const originallyPublishedJob = await queues.incomingEvents.add(
      JOB_NAMES.serviceRequestIngested,
      incomingJobData,
      {
        jobId: incomingJobId,
      },
    );

    expect(originallyPublishedJob.id).toBe(incomingJobId);

    expect(await originallyPublishedJob.getState()).toBe("waiting");

    const stillPendingOutbox = await database.outboxEvent.findUniqueOrThrow({
      where: {
        id: outboxEventId,
      },
    });

    expect(stillPendingOutbox.status).toBe("PENDING");

    expect(stillPendingOutbox.attemptCount).toBe(0);

    /*
     * A later publisher cycle sees the durable event still PENDING and calls
     * queue.add() again with the same deterministic job ID.
     */
    const publisher = new InternalOutboxPublisher({
      database,
      incomingQueue: queues.incomingEvents,
      logger,
      pollIntervalMs: 100,
      batchSize: 1,
      now: () => publicationNow,
    });

    const repeatedPublication = await publisher.publishOnce();

    expect(repeatedPublication).toEqual({
      selected: 1,
      published: 1,
      failed: 0,
    });

    const convergedIncomingJob =
      await queues.incomingEvents.getJob(incomingJobId);

    expect(convergedIncomingJob).not.toBeNull();

    if (!convergedIncomingJob) {
      throw new Error(
        "Expected the deterministic incoming job to remain available",
      );
    }

    expect(convergedIncomingJob.id).toBe(originallyPublishedJob.id);

    expect(convergedIncomingJob.timestamp).toBe(
      originallyPublishedJob.timestamp,
    );

    expect(convergedIncomingJob.data).toEqual(incomingJobData);

    expect(await countQueueJobs(queues.incomingEvents)).toBe(1);

    const deliveredOutbox = await database.outboxEvent.findUniqueOrThrow({
      where: {
        id: outboxEventId,
      },
    });

    expect(deliveredOutbox.status).toBe("DELIVERED");

    expect(deliveredOutbox.attemptCount).toBe(1);

    expect(deliveredOutbox.processedAt).toEqual(publicationNow);

    expect(deliveredOutbox.nextAttemptAt).toEqual(publicationNow);

    expect(deliveredOutbox.lastError).toBeNull();

    /*
     * Start the real worker only after duplicate publication has converged.
     */
    incomingWorker = createIncomingWorker({
      database,
      routingQueue: queues.routing,
      deadLetterQueue: queues.deadLetter,
      logger,
      redisUrl,
      concurrency: 1,
    });

    incomingWorker.on("error", (error) => {
      logger.error(
        {
          err: error,
        },
        "Duplicate-publication test worker error",
      );
    });

    await incomingWorker.waitUntilReady();

    await waitForJobState(convergedIncomingJob, "completed");

    const routingJobId = createRoutingJobId(serviceRequestId);

    const routingJob = await waitForJob<RouteServiceRequestJobData>(
      queues.routing,
      routingJobId,
    );

    expect(routingJob.name).toBe(JOB_NAMES.routeServiceRequest);

    expect(routingJob.data).toEqual({
      organizationId,
      serviceRequestId,
      correlationId,
    });

    expect(await routingJob.getState()).toBe("waiting");

    expect(await countQueueJobs(queues.routing)).toBe(1);

    const unchangedRequest = await database.serviceRequest.findUniqueOrThrow({
      where: {
        id: serviceRequestId,
      },
    });

    expect(unchangedRequest.status).toBe("PENDING");

    expect(await countQueueJobs(queues.deadLetter)).toBe(0);

    expect(queues.incomingEvents.name).toBe(QUEUE_NAMES.incomingEvents);
  }, 10_000);
});
